package org.apache.spark.sql.lcpy

import java.io.{ByteArrayOutputStream, DataOutputStream, FilterOutputStream, IOException, OutputStream}
import java.nio.channels.Channels
import java.util.Base64
import java.util.concurrent.{LinkedBlockingQueue, Semaphore, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{FieldVector, IntVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, WriteChannel}
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.arrow.LcArrowIngest
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}

/** One JVM-wide root; every task gets a child allocator that must be empty when it closes. */
object LcMemory extends Logging {
  lazy val root: RootAllocator = new RootAllocator(Long.MaxValue)
  val leaks = new AtomicLong()
}

/**
 * Task-scoped allocator shared by the writer thread and the task thread. Reference counted: it is
 * closed (and checked for leaks) by whichever of the two finishes last, so a writer stuck inside
 * an upstream native call can never have memory freed under it.
 */
final class TaskArrow(name: String, failOnLeak: Boolean) extends Logging {
  val alloc: BufferAllocator = LcMemory.root.newChildAllocator(name, 0, Long.MaxValue)
  private val refs = new AtomicInteger(1)
  def retain(): Unit = refs.incrementAndGet()
  def release(): Unit = if (refs.decrementAndGet() == 0) {
    val outstanding = alloc.getAllocatedMemory
    if (outstanding != 0) {
      LcMemory.leaks.incrementAndGet()
      val msg = s"lcpy: $name leaked $outstanding bytes of Arrow memory\n${alloc.toVerboseString}"
      if (failOnLeak) throw new IllegalStateException(msg) else logError(msg)
    }
    try alloc.close() catch { case NonFatal(e) => if (failOnLeak) throw e else logError("lcpy", e) }
  }
}

/** Columns we copied out of an upstream batch, owned by the task allocator. */
final class OwnedBatch(val vectors: Array[FieldVector], val rows: Int) extends AutoCloseable {
  override def close(): Unit = vectors.foreach(v => if (v != null) v.close())
}

/** Counters touched by the writer thread; folded into SQLMetrics on the task thread. */
final class RunnerStats {
  val inputRows = new AtomicLong()
  val inputBatches = new AtomicLong()
  val bytesSent = new AtomicLong()
  val flushes = new AtomicLong()
}

private final class CountingStream(out: OutputStream, counter: AtomicLong)
  extends FilterOutputStream(out) {
  override def write(b: Int): Unit = { out.write(b); counter.incrementAndGet() }
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    out.write(b, off, len); counter.addAndGet(len)
  }
}

/** Upstream input: either columnar batches (Comet or Spark) or rows. */
sealed trait LcInput
final case class ColumnarInput(it: Iterator[ColumnarBatch]) extends LcInput
final case class RowInput(it: Iterator[InternalRow]) extends LcInput

/** What one call needs on the wire: which wire columns, under which names. */
final case class WireCall(spec: FunctionSpec, columns: Seq[Int], names: Seq[String])

/**
 * Shared machinery: spawn the worker, write the header and an Arrow IPC stream on a writer
 * thread, read the result stream on the task thread, watch for task kill, and tear everything
 * down exactly once.
 *
 * Flushing policy: the JVM->worker pipe is a large buffer that is flushed only when the writer is
 * about to block (back-pressure) and at end of input -- never on a timer. The worker mirrors this
 * (it flushes its output only when its input has no data ready). Neither side ever waits for data
 * that the other side is still holding in a buffer, so no deadlock is possible on small
 * partitions, and large partitions stream with full buffers.
 */
abstract class LcRunnerBase(
    conf: RunnerConf,
    val label: String,
    metrics: Map[String, SQLMetric]) extends Iterator[ColumnarBatch] with Logging {

  protected val ctx: TaskContext = TaskContext.get()
  protected val mem = new TaskArrow(s"lcpy-$label-${if (ctx != null) ctx.taskAttemptId() else 0}",
    conf.failOnLeak)
  protected val stats = new RunnerStats
  protected val worker = new WorkerProcess(conf, label)
  private val pipe = new DataOutputStream(new CountingStream(worker.stdin, stats.bytesSent))

  @volatile protected var writerError: Throwable = null
  @volatile protected var closed = false
  @volatile private var finished = false
  private var reader: ArrowStreamReader = _
  private var current: ColumnarBatch = _
  private var lookahead: ColumnarBatch = _
  private var outputRows = 0L

  if (ctx != null) ctx.addTaskCompletionListener[Unit](_ => close())

  // ---- subclass contract
  protected def mode: Mode
  protected def global: Boolean
  protected def calls: Seq[WireCall]
  protected def wireFields: Seq[Field]
  protected def gidColumn: Int
  /** Writer thread body: ingest upstream input and call `send` for each wire batch. */
  protected def produce(): Unit
  /** Task thread: pair a freshly read result batch with its queued input. */
  protected def assemble(results: Array[FieldVector], rows: Int): ColumnarBatch
  /** Task thread at end of stream: verify every input was answered. */
  protected def verifyComplete(): Unit
  /** Release anything still queued. */
  protected def drainQueues(): Unit

  // ---- wire format
  protected def tz: String = conf.sessionTimeZone
  protected def fieldFor(name: String, dt: DataType): Field =
    ArrowUtils.toArrowField(name, dt, nullable = true, tz)
  protected def gidField: Field =
    new Field("__lcpy_gid", FieldType.nullable(new ArrowType.Int(32, true)), null)

  private lazy val channel = new WriteChannel(Channels.newChannel(pipe))
  private lazy val wireSchema = new Schema(wireFields.asJava)

  private def headerJson(): Array[Byte] = {
    val m = new ObjectMapper()
    val h = m.createObjectNode()
    h.put("version", 1)
    h.put("mode", mode.wire)
    h.put("global", global)
    h.put("gid_column", gidColumn)
    h.put("label", label)
    val outFields = calls.zipWithIndex.map { case (c, i) => fieldFor(s"r$i", c.spec.returnType) }
    h.put("output_schema", Base64.getEncoder.encodeToString(schemaBytes(new Schema(outFields.asJava))))
    val arr = h.putArray("calls")
    calls.foreach { c =>
      val o: ObjectNode = arr.addObject()
      o.put("module", c.spec.module)
      o.put("function", c.spec.function)
      o.put("sha256", c.spec.moduleSha256)
      val cols = o.putArray("columns"); c.columns.foreach(i => cols.add(i))
      val names = o.putArray("names"); c.names.foreach(n => names.add(n))
    }
    m.writeValueAsBytes(h)
  }

  private def schemaBytes(s: Schema): Array[Byte] = {
    val bo = new ByteArrayOutputStream()
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(bo)), s)
    bo.toByteArray
  }

  /** Writer thread: serialize one record batch of wire vectors into the pipe buffer. */
  protected def send(vectors: Seq[FieldVector], rows: Int): Unit = {
    val root = new VectorSchemaRoot(vectors.map(_.getField).asJava, vectors.asJava, rows)
    val rb = new VectorUnloader(root).getRecordBatch
    try MessageSerializer.serialize(channel, rb) finally rb.close()
    stats.inputBatches.incrementAndGet()
    stats.inputRows.addAndGet(rows)
  }

  /** Writer thread: called right before blocking on back-pressure. */
  protected def flushBeforeBlocking(): Unit = { pipe.flush(); stats.flushes.incrementAndGet() }

  @volatile protected var produced = false

  protected def checkWriterAlive(): Unit = {
    if (closed) throw new InterruptedException("lcpy runner closed")
    if (!worker.isAlive) throw new IOException("worker process exited while input was pending")
  }

  private val writer: Thread = new Thread(s"lcpy-writer-${worker.id}") {
    override def run(): Unit = {
      if (ctx != null) TaskContext.setTaskContext(ctx)
      try {
        val header = headerJson()
        pipe.writeInt(header.length)
        pipe.write(header)
        MessageSerializer.serialize(channel, wireSchema)
        produce()
        produced = true
        channel.writeIntLittleEndian(0xFFFFFFFF) // IPC end-of-stream
        channel.writeIntLittleEndian(0)
        pipe.flush()
        stats.flushes.incrementAndGet()
        worker.stdin.close()
      } catch {
        case t: Throwable if closed => logDebug("lcpy writer stopped after close", t)
        case t: Throwable =>
          writerError = t
          // Make the task thread notice: it may be blocked reading the worker's output.
          worker.kill()
      } finally {
        mem.release()
        if (ctx != null) TaskContext.unset()
      }
    }
  }
  writer.setDaemon(true)

  /** Kills the worker if Spark interrupts the task: a blocking pipe read is not interruptible. */
  private val monitor: Thread = new Thread(s"lcpy-monitor-${worker.id}") {
    override def run(): Unit = {
      while (!closed && !finished) {
        if (ctx != null && ctx.isInterrupted()) { worker.kill(); return }
        try Thread.sleep(100) catch { case _: InterruptedException => return }
      }
    }
  }
  monitor.setDaemon(true)

  /** Start the threads. Called once the subclass is fully constructed. */
  def start(): this.type = {
    mem.retain()
    writer.start()
    monitor.start()
    this
  }

  // ---- reading (task thread)

  protected def fail(what: String, cause: Throwable = null): Nothing = {
    if (ctx != null) ctx.killTaskIfInterrupted()
    val w = writerError
    if (w != null && !w.isInstanceOf[IOException]) {
      // Upstream (e.g. a Comet operator) failed: surface the real cause, not the broken pipe.
      throw w
    }
    throw new LcPythonWorkerException(worker.describeFailure(what), Option(cause).getOrElse(w))
  }

  private def ensureReader(): ArrowStreamReader = {
    if (reader == null) {
      reader = new ArrowStreamReader(worker.stdout, mem.alloc)
      val got = try reader.getVectorSchemaRoot.getSchema catch {
        case NonFatal(e) => fail("worker failed before producing any result", e)
      }
      val expected = calls.indices.map(i => s"r$i")
      if (got.getFields.asScala.map(_.getName) != expected) {
        fail(s"worker produced output columns ${got.getFields.asScala.map(_.getName)}, " +
          s"expected $expected")
      }
    }
    reader
  }

  private def fetch(): ColumnarBatch = {
    val r = ensureReader()
    val ok = try r.loadNextBatch() catch {
      case NonFatal(e) => fail("error reading worker output", e)
    }
    if (!ok) {
      finished = true
      worker.waitExit(30000) match {
        case Some(0) =>
        case Some(_) => fail("worker exited with an error")
        case None => fail("worker closed its output but did not exit")
      }
      writer.join(30000)
      if (writerError != null) fail("error while feeding the worker")
      verifyComplete()
      return null
    }
    val root = r.getVectorSchemaRoot
    val rows = root.getRowCount
    val results = root.getFieldVectors.asScala.map { v =>
      val tp = v.getTransferPair(mem.alloc)
      tp.transfer()
      tp.getTo.asInstanceOf[FieldVector]
    }.toArray
    val b = try assemble(results, rows) catch {
      case t: Throwable => results.foreach(_.close()); throw t
    }
    outputRows += b.numRows()
    b
  }

  override def hasNext: Boolean = {
    if (lookahead != null) return true
    if (finished || closed) return false
    if (current != null) { current.close(); current = null }
    lookahead = fetch()
    if (lookahead == null) { publishMetrics(); false } else true
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException
    current = lookahead
    lookahead = null
    current
  }

  private var published = false
  private def publishMetrics(): Unit = if (!published) {
    published = true
    def add(k: String, v: Long): Unit = metrics.get(k).foreach(_.add(v))
    add("numOutputRows", outputRows)
    add("numInputRows", stats.inputRows.get())
    add("numInputBatches", stats.inputBatches.get())
    add("bytesSent", stats.bytesSent.get())
    add("pipeFlushes", stats.flushes.get())
    add("workerSpawnTime", worker.spawnNanos / 1000000)
    add("workersLaunched", 1)
  }

  /** Exactly-once teardown, from task completion (success, failure or kill). */
  def close(): Unit = synchronized {
    if (closed) return
    val clean = finished && writerError == null
    closed = true
    publishMetrics()
    try {
      Seq(lookahead, current).foreach(b => if (b != null) b.close())
      lookahead = null; current = null
      worker.close(if (clean) conf.shutdownGraceMs else 0L)
      writer.interrupt()
      writer.join(10000)
      if (writer.isAlive) logWarning(s"lcpy writer ${writer.getName} still busy after close; " +
        "its allocator will be released when it returns")
      monitor.interrupt()
      drainQueues()
      if (reader != null) try reader.close() catch { case NonFatal(_) => }
    } finally {
      mem.release()
    }
  }
}

/**
 * 1-to-1 runner. Every upstream column is copied once into owned vectors; the argument columns
 * are streamed to the worker and the copies are queued (in order) until the matching result
 * batch comes back, then emitted together. In-flight batches are bounded by a semaphore.
 */
final class RowModeRunner(
    spec: FunctionSpec,
    childNames: Seq[String],
    childTypes: Seq[DataType],
    argOrdinals: Seq[Int],
    argNames: Seq[String],
    input: LcInput,
    conf: RunnerConf,
    metrics: Map[String, SQLMetric]) extends LcRunnerBase(conf, spec.qualifiedName, metrics) {

  private val wireOrdinals: Seq[Int] = argOrdinals.distinct
  private val queue = new LinkedBlockingQueue[AnyRef]()
  private val inflight = new Semaphore(conf.maxInflight)
  private case object End

  override protected def mode: Mode = Mode.Row
  override protected def global: Boolean = false
  override protected def gidColumn: Int = -1
  override protected lazy val calls: Seq[WireCall] =
    Seq(WireCall(spec, argOrdinals.map(wireOrdinals.indexOf), argNames))
  override protected lazy val wireFields: Seq[Field] =
    wireOrdinals.map(i => fieldFor(s"c$i", childTypes(i)))

  private def acquire(): Unit = {
    if (!inflight.tryAcquire()) {
      flushBeforeBlocking()
      while (!inflight.tryAcquire(100, TimeUnit.MILLISECONDS)) checkWriterAlive()
    }
  }

  private def ingestColumnar(b: ColumnarBatch): OwnedBatch = {
    val n = b.numRows()
    val vs = new Array[FieldVector](childTypes.size)
    try {
      var i = 0
      while (i < vs.length) {
        vs(i) = LcArrowIngest.copyColumn(b.column(i), n, s"c$i", childTypes(i), tz, mem.alloc)
        i += 1
      }
      new OwnedBatch(vs, n)
    } catch { case t: Throwable => new OwnedBatch(vs, n).close(); throw t }
  }

  private def ingestRows(it: Iterator[InternalRow]): OwnedBatch = {
    val vs = childTypes.zipWithIndex.map { case (dt, i) =>
      LcArrowIngest.newVector(s"c$i", dt, tz, mem.alloc) }
    val root = new VectorSchemaRoot(vs.map(_.getField).asJava, vs.asJava, 0)
    try {
      val app = new LcArrowIngest.RowAppender(root)
      var n = 0
      while (n < conf.batchRows && it.hasNext) { app.write(it.next()); n += 1 }
      app.finish()
      new OwnedBatch(vs.toArray, n)
    } catch { case t: Throwable => vs.foreach(_.close()); throw t }
  }

  private def enqueueAndSend(owned: OwnedBatch): Unit = {
    queue.put(owned) // before sending: the answer can never arrive before its input is queued
    send(wireOrdinals.map(owned.vectors(_)), owned.rows)
  }

  override protected def produce(): Unit = {
    input match {
      case ColumnarInput(it) =>
        while (it.hasNext) {
          checkWriterAlive()
          val b = it.next()
          if (b.numRows() > 0) {
            acquire()
            enqueueAndSend(ingestColumnar(b)) // copied before `it` is advanced again
          }
        }
      case RowInput(it) =>
        while (it.hasNext) {
          checkWriterAlive()
          acquire()
          val owned = ingestRows(it)
          if (owned.rows > 0) enqueueAndSend(owned) else { owned.close(); inflight.release() }
        }
    }
    queue.put(End)
  }

  private def take(): AnyRef = {
    var o = queue.poll(100, TimeUnit.MILLISECONDS)
    while (o == null) {
      if (writerError != null) fail("error while feeding the worker")
      if (closed) throw new InterruptedException("closed")
      o = queue.poll(100, TimeUnit.MILLISECONDS)
    }
    o
  }

  override protected def assemble(results: Array[FieldVector], rows: Int): ColumnarBatch = {
    take() match {
      case End => fail(s"worker returned more batches than it was sent")
      case owned: OwnedBatch =>
        if (owned.rows != rows) {
          owned.close()
          fail(s"row-preserving function ${spec.qualifiedName} returned $rows rows for a batch " +
            s"of ${owned.rows} input rows (1-to-1 contract violated)")
        }
        inflight.release()
        val cols: Array[ColumnVector] =
          (owned.vectors.map(v => new ArrowColumnVector(v): ColumnVector) ++
            results.map(v => new ArrowColumnVector(v): ColumnVector))
        new ColumnarBatch(cols, rows)
    }
  }

  override protected def verifyComplete(): Unit = take() match {
    case End =>
    case owned: OwnedBatch =>
      owned.close()
      fail(s"worker finished without answering all input batches")
  }

  override protected def drainQueues(): Unit = {
    var o = queue.poll()
    while (o != null) {
      o match { case b: OwnedBatch => b.close(); case _ => }
      o = queue.poll()
    }
  }
}

/**
 * N-to-1 runner. Input arrives sorted by the grouping keys (the operator requires it), so groups
 * are contiguous: the writer tags each row with a running group id and queues each group's key
 * once; the worker assembles one group at a time and answers one row per group. Only the current
 * group is ever materialised (in Python); the JVM holds the in-flight keys, bounded by
 * `spark.lcpy.groups.maxInflight` (+ at most one batch of new groups).
 */
final class GroupedRunner(
    specs: Seq[FunctionSpec],
    argOrdinals: Seq[Seq[Int]],
    argNames: Seq[Seq[String]],
    childTypes: Seq[DataType],
    keyProjection: () => UnsafeProjection,
    keyTypes: Seq[DataType],
    input: LcInput,
    conf: RunnerConf,
    metrics: Map[String, SQLMetric])
  extends LcRunnerBase(conf, specs.map(_.qualifiedName).mkString("+"), metrics) {

  private val isGlobal = keyTypes.isEmpty
  private val wireOrdinals: Seq[Int] = argOrdinals.flatten.distinct
  private val keys = new LinkedBlockingQueue[UnsafeRow]()
  private val room = new Object
  private val emptyKey: UnsafeRow = UnsafeProjection.create(new StructType()).apply(InternalRow.empty).copy()

  override protected def mode: Mode = Mode.Grouped
  override protected def global: Boolean = isGlobal
  override protected def gidColumn: Int = wireOrdinals.size
  override protected lazy val calls: Seq[WireCall] = specs.indices.map { i =>
    WireCall(specs(i), argOrdinals(i).map(wireOrdinals.indexOf), argNames(i))
  }
  override protected lazy val wireFields: Seq[Field] =
    wireOrdinals.map(i => fieldFor(s"c$i", childTypes(i))) :+ gidField

  private def waitForRoom(): Unit = {
    if (keys.size() > conf.maxInflightGroups) {
      flushBeforeBlocking()
      room.synchronized {
        while (keys.size() > conf.maxInflightGroups / 2) { checkWriterAlive(); room.wait(50) }
      }
    }
  }

  override protected def produce(): Unit = {
    val proj = keyProjection()
    var gid = -1
    var prev: UnsafeRow = null
    if (isGlobal) { keys.put(emptyKey); gid = 0 }

    def tag(row: InternalRow, gids: IntVector, i: Int): Unit = {
      if (!isGlobal) {
        val k = proj(row)
        if (prev == null || !k.equals(prev)) {
          gid += 1
          prev = k.copy()
          keys.put(prev)
        }
      }
      gids.set(i, gid)
    }

    def emit(wire: Seq[FieldVector], gids: IntVector, n: Int): Unit = {
      try send(wire :+ gids, n) finally { wire.foreach(_.close()); gids.close() }
    }

    input match {
      case ColumnarInput(it) =>
        while (it.hasNext) {
          checkWriterAlive()
          val b = it.next()
          val n = b.numRows()
          if (n > 0) {
            waitForRoom()
            val gids = new IntVector("__lcpy_gid", mem.alloc)
            gids.allocateNew(n)
            val rowsIt = b.rowIterator()
            var i = 0
            while (rowsIt.hasNext) { tag(rowsIt.next(), gids, i); i += 1 }
            gids.setValueCount(n)
            val wire = wireOrdinals.map(o => LcArrowIngest.copyColumn(
              b.column(o), n, s"c$o", childTypes(o), tz, mem.alloc))
            emit(wire, gids, n)
          }
        }
      case RowInput(it) =>
        val bound = UnsafeProjection.create(
          wireOrdinals.map(o => org.apache.spark.sql.catalyst.expressions.BoundReference(
            o, childTypes(o), nullable = true)))
        while (it.hasNext) {
          checkWriterAlive()
          waitForRoom()
          val vs = wireOrdinals.map(o => LcArrowIngest.newVector(s"c$o", childTypes(o), tz, mem.alloc))
          val root = new VectorSchemaRoot(vs.map(_.getField).asJava, vs.asJava, 0)
          val app = new LcArrowIngest.RowAppender(root)
          val gids = new IntVector("__lcpy_gid", mem.alloc)
          gids.allocateNew(conf.batchRows)
          var n = 0
          while (n < conf.batchRows && it.hasNext) {
            val row = it.next()
            tag(row, gids, n)
            app.write(bound(row))
            n += 1
          }
          app.finish()
          gids.setValueCount(n)
          if (n > 0) emit(vs, gids, n) else { vs.foreach(_.close()); gids.close() }
        }
    }
  }

  override protected def assemble(results: Array[FieldVector], rows: Int): ColumnarBatch = {
    val ks = new Array[UnsafeRow](rows)
    var i = 0
    while (i < rows) {
      var k = keys.poll(100, TimeUnit.MILLISECONDS)
      while (k == null) {
        if (writerError != null) fail("error while feeding the worker")
        if (produced && keys.isEmpty) {
          results.foreach(_.close())
          fail(s"worker returned more group results than groups it was sent")
        }
        k = keys.poll(100, TimeUnit.MILLISECONDS)
      }
      ks(i) = k
      i += 1
    }
    room.synchronized(room.notifyAll())
    val keyVecs = keyTypes.zipWithIndex.map { case (dt, j) =>
      LcArrowIngest.newVector(s"k$j", dt, tz, mem.alloc) }
    if (keyVecs.nonEmpty) {
      val root = new VectorSchemaRoot(keyVecs.map(_.getField).asJava, keyVecs.asJava, 0)
      val app = new LcArrowIngest.RowAppender(root)
      ks.foreach(app.write)
      app.finish()
    }
    val cols: Array[ColumnVector] =
      (keyVecs.map(v => new ArrowColumnVector(v): ColumnVector) ++
        results.map(v => new ArrowColumnVector(v): ColumnVector)).toArray
    new ColumnarBatch(cols, rows)
  }

  override protected def verifyComplete(): Unit = {
    if (!keys.isEmpty) {
      fail(s"worker finished without answering ${keys.size()} group(s)")
    }
  }

  override protected def drainQueues(): Unit = keys.clear()
}
