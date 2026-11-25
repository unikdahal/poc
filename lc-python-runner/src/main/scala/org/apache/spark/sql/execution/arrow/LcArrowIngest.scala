package org.apache.spark.sql.execution.arrow

import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, BitVector, FieldVector, ValueVector, VectorSchemaRoot}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Turns whatever the upstream operator produced into Arrow vectors that WE own.
 *
 * Ownership model: an upstream batch is *borrowed* -- valid only until the upstream iterator is
 * advanced (Comet's native operators and Spark's vectorized readers reuse buffers across batches).
 * Everything that must outlive that window (the pass-through columns that are emitted next to the
 * Python result, and the arguments queued for the pipe) is copied into vectors allocated from the
 * task's own allocator, before the upstream iterator is touched again.
 *
 * Copy strategies, per column, cheapest first:
 *  1. raw buffer copy: validity / offsets / data memcpy'd by address. Used for flat types when the
 *     source is an Arrow vector with the same physical layout. Works for Spark's own
 *     ArrowColumnVector and, via reflection, for Comet's CometVector, whose Arrow classes are
 *     shaded (org.apache.comet.shaded.arrow) and so cannot be passed to Spark's Arrow directly.
 *  2. columnar getter copy: Spark's ArrowFieldWriter driven by the column's typed getters, one
 *     column at a time (dictionary-encoded, nested, or layout-mismatched columns).
 * Neither path materialises InternalRows. Row-based input goes through Spark's ArrowWriter.
 */
object LcArrowIngest {

  /** Stats for tests / metrics: how many columns took each path. */
  val rawCopies = new java.util.concurrent.atomic.AtomicLong()
  val getterCopies = new java.util.concurrent.atomic.AtomicLong()

  def newVector(name: String, dt: DataType, tz: String, alloc: BufferAllocator): FieldVector =
    ArrowUtils.toArrowField(name, dt, nullable = true, tz).createVector(alloc)

  /** Copy column `src` (first `n` rows) into a fresh vector named `name`. */
  def copyColumn(
      src: ColumnVector,
      n: Int,
      name: String,
      dt: DataType,
      tz: String,
      alloc: BufferAllocator): FieldVector = {
    val target = newVector(name, dt, tz, alloc)
    try {
      val raw = RawSource.of(src)
      if (raw.isDefined && rawCopy(raw.get, n, target)) {
        rawCopies.incrementAndGet()
      } else {
        target.clear()
        getterCopy(src, n, target)
        getterCopies.incrementAndGet()
      }
      target
    } catch {
      case t: Throwable => target.close(); throw t
    }
  }

  def getterCopy(src: ColumnVector, n: Int, target: FieldVector): Unit = {
    target.allocateNew()
    val w = ArrowWriter.createFieldWriter(target)
    val g = new ColumnGetters(src)
    var i = 0
    while (i < n) { w.write(g, i); i += 1 }
    w.finish()
  }

  /** Row input: append rows to a root of freshly allocated vectors. */
  class RowAppender(root: VectorSchemaRoot) {
    private val writer = ArrowWriter.create(root)
    def write(row: InternalRow): Unit = writer.write(row)
    def finish(): Unit = writer.finish()
  }

  // -------------------------------------------------------------------------- raw copy

  private def flatMinor(dt: DataType): Option[String] = dt match {
    case BooleanType => Some("BIT")
    case ByteType => Some("TINYINT")
    case ShortType => Some("SMALLINT")
    case IntegerType => Some("INT")
    case LongType => Some("BIGINT")
    case FloatType => Some("FLOAT4")
    case DoubleType => Some("FLOAT8")
    case DateType => Some("DATEDAY")
    case TimestampType => Some("TIMESTAMPMICROTZ")
    case TimestampNTZType => Some("TIMESTAMPMICRO")
    case _: DecimalType => Some("DECIMAL")
    case StringType => Some("VARCHAR")
    case BinaryType => Some("VARBINARY")
    case _ => None
  }

  private def rawCopy(src: RawSource, n: Int, target: FieldVector): Boolean = {
    val dtMinor = target.getMinorType.name()
    if (src.hasDictionary || src.minorType != dtMinor) return false
    if (dtMinor == "DECIMAL" && src.typeString != target.getField.getType.toString) return false
    if (src.valueCount < n) return false
    target match {
      case bits: BitVector =>
        bits.allocateNew(n)
        copyValidity(src, n, bits.getValidityBufferAddress)
        Platform.copyMemory(null, src.dataAddress, null, bits.getDataBufferAddress, bitmapBytes(n))
        bits.setValueCount(n)
        true
      case fw: BaseFixedWidthVector =>
        fw.allocateNew(n)
        copyValidity(src, n, fw.getValidityBufferAddress)
        Platform.copyMemory(null, src.dataAddress, null, fw.getDataBufferAddress,
          n.toLong * fw.getTypeWidth)
        fw.setValueCount(n)
        true
      case vw: BaseVariableWidthVector =>
        val so = src.offsetAddress
        val start = Platform.getInt(null, so)
        val end = Platform.getInt(null, so + 4L * n)
        val len = end - start
        if (len < 0) return false
        vw.allocateNew(math.max(len, 1).toLong, n)
        copyValidity(src, n, vw.getValidityBufferAddress)
        val to = vw.getOffsetBufferAddress
        if (start == 0) {
          Platform.copyMemory(null, so, null, to, 4L * (n + 1))
        } else {
          var i = 0
          while (i <= n) {
            Platform.putInt(null, to + 4L * i, Platform.getInt(null, so + 4L * i) - start)
            i += 1
          }
        }
        if (len > 0) {
          Platform.copyMemory(null, src.dataAddress + start, null, vw.getDataBufferAddress, len)
        }
        vw.setLastSet(n - 1)
        vw.setValueCount(n)
        true
      case _ => false
    }
  }

  private def bitmapBytes(n: Int): Long = (n + 7) / 8

  private def copyValidity(src: RawSource, n: Int, dst: Long): Unit = {
    if (src.nullCount == 0) {
      Platform.setMemory(dst, 0xff.toByte, bitmapBytes(n))
    } else {
      Platform.copyMemory(null, src.validityAddress, null, dst, bitmapBytes(n))
    }
  }

  /** Address-level view of an Arrow vector, shaded or not. */
  private[arrow] trait RawSource {
    def minorType: String
    def typeString: String
    def hasDictionary: Boolean
    def valueCount: Int
    def nullCount: Int
    def validityAddress: Long
    def dataAddress: Long
    def offsetAddress: Long
  }

  private[arrow] object RawSource {
    private val CometVectorClass = "org.apache.comet.vector.CometVector"

    def of(cv: ColumnVector): Option[RawSource] = cv match {
      case a: ArrowColumnVector => a.getValueVector match {
        case fv: FieldVector => Some(new Unshaded(fv))
        case _ => None
      }
      case other if isComet(other.getClass) =>
        if (other.getClass.getSimpleName.contains("Dictionary")) None
        else Reflective.valueVector(other).map(new Reflective(_))
      case _ => None
    }

    private val cometCache = new ConcurrentHashMap[Class[_], java.lang.Boolean]()
    private def isComet(c: Class[_]): Boolean = cometCache.computeIfAbsent(c, { cls =>
      var k: Class[_] = cls
      var found = false
      while (k != null && !found) { found = k.getName == CometVectorClass; k = k.getSuperclass }
      java.lang.Boolean.valueOf(found)
    }).booleanValue()

    final class Unshaded(v: FieldVector) extends RawSource {
      def minorType: String = v.getMinorType.name()
      def typeString: String = v.getField.getType.toString
      def hasDictionary: Boolean = v.getField.getDictionary != null
      def valueCount: Int = v.getValueCount
      def nullCount: Int = v.getNullCount
      def validityAddress: Long = v.getValidityBufferAddress
      def dataAddress: Long = v.getDataBufferAddress
      def offsetAddress: Long = v.getOffsetBufferAddress
    }

    /** Calls the same Arrow methods on a shaded vector through cached reflection. */
    final class Reflective(v: AnyRef) extends RawSource {
      private def call(name: String): AnyRef = Reflective.method(v.getClass, name).invoke(v)
      private lazy val field = call("getField")
      def minorType: String = call("getMinorType").asInstanceOf[Enum[_]].name()
      def typeString: String =
        Reflective.method(field.getClass, "getType").invoke(field).toString
      def hasDictionary: Boolean =
        Reflective.method(field.getClass, "getDictionary").invoke(field) != null
      def valueCount: Int = call("getValueCount").asInstanceOf[Int]
      def nullCount: Int = call("getNullCount").asInstanceOf[Int]
      def validityAddress: Long = call("getValidityBufferAddress").asInstanceOf[Long]
      def dataAddress: Long = call("getDataBufferAddress").asInstanceOf[Long]
      def offsetAddress: Long = call("getOffsetBufferAddress").asInstanceOf[Long]
    }

    object Reflective {
      private val methods = new ConcurrentHashMap[(Class[_], String), Method]()
      def method(c: Class[_], name: String): Method =
        methods.computeIfAbsent((c, name), { case (cls, n) =>
          val m = cls.getMethod(n)
          m.setAccessible(true)
          m
        })
      def valueVector(cometVector: ColumnVector): Option[AnyRef] =
        try Option(method(cometVector.getClass, "getValueVector").invoke(cometVector))
        catch { case _: Exception => None }
    }
  }
}

/** Typed getters of one column, addressed by row id, in the shape ArrowFieldWriter consumes. */
final class ColumnGetters(cv: ColumnVector) extends SpecializedGetters {
  override def isNullAt(ordinal: Int): Boolean = cv.isNullAt(ordinal)
  override def getBoolean(ordinal: Int): Boolean = cv.getBoolean(ordinal)
  override def getByte(ordinal: Int): Byte = cv.getByte(ordinal)
  override def getShort(ordinal: Int): Short = cv.getShort(ordinal)
  override def getInt(ordinal: Int): Int = cv.getInt(ordinal)
  override def getLong(ordinal: Int): Long = cv.getLong(ordinal)
  override def getFloat(ordinal: Int): Float = cv.getFloat(ordinal)
  override def getDouble(ordinal: Int): Double = cv.getDouble(ordinal)
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    cv.getDecimal(ordinal, precision, scale)
  override def getUTF8String(ordinal: Int): UTF8String = cv.getUTF8String(ordinal)
  override def getBinary(ordinal: Int): Array[Byte] = cv.getBinary(ordinal)
  override def getInterval(ordinal: Int): CalendarInterval = cv.getInterval(ordinal)
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = cv.getStruct(ordinal)
  override def getArray(ordinal: Int): ArrayData = cv.getArray(ordinal)
  override def getMap(ordinal: Int): MapData = cv.getMap(ordinal)
  override def get(ordinal: Int, dataType: DataType): AnyRef =
    org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader.read(
      this, ordinal, dataType, true, true)
}
