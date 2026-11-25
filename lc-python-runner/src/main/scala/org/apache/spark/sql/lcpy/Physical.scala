package org.apache.spark.sql.lcpy

import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/** Common behaviour of both operators: accept rows or columns, produce either. */
trait LcExecBase extends UnaryExecNode {
  def runnerConf: RunnerConf

  // Columnar by default; row output is also supported natively (see doExecute) so that Spark
  // never needs to wrap us in a transition when a row-based parent asks for rows.
  override def supportsColumnar: Boolean = true
  override def supportsRowBased: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of rows sent to Python"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of batches sent to Python"),
    "bytesSent" -> SQLMetrics.createSizeMetric(sparkContext, "bytes sent to Python"),
    "pipeFlushes" -> SQLMetrics.createMetric(sparkContext, "pipe flushes"),
    "workersLaunched" -> SQLMetrics.createMetric(sparkContext, "sandboxed workers launched"),
    "workerSpawnTime" -> SQLMetrics.createTimingMetric(sparkContext, "worker spawn time"))

  /** Implemented by each operator; both doExecuteColumnar and doExecute delegate here. */
  def runColumnar(): RDD[ColumnarBatch]
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = runColumnar()

  protected def input(): Either[RDD[ColumnarBatch], RDD[InternalRow]] =
    if (child.supportsColumnar) Left(child.executeColumnar()) else Right(child.execute())

  /** Row output for row-based parents / direct `execute()` callers (e.g. some subqueries). */
  override protected def doExecute(): RDD[InternalRow] = {
    val out = output
    runColumnar().mapPartitionsInternal { batches =>
      val proj = UnsafeProjection.create(out, out)
      batches.flatMap { b =>
        import scala.collection.JavaConverters._
        b.rowIterator().asScala.map(proj)
      }
    }
  }
}

case class LcPythonEvalExec(
    spec: FunctionSpec,
    argAttrs: Seq[Attribute],
    argNames: Seq[String],
    resultAttr: Attribute,
    child: SparkPlan,
    runnerConf: RunnerConf) extends LcExecBase {

  override def output: Seq[Attribute] = child.output :+ resultAttr
  override def producedAttributes: AttributeSet = AttributeSet(resultAttr :: Nil)
  override lazy val references: AttributeSet = AttributeSet(argAttrs)
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def simpleString(maxFields: Int): String =
    s"LcPythonEval ${LcNodes.describe(spec, argAttrs)} -> $resultAttr"

  override def runColumnar(): RDD[ColumnarBatch] = {
    val childOut = child.output
    val ordinals = argAttrs.map(a => childOut.indexWhere(_.exprId == a.exprId))
    require(ordinals.forall(_ >= 0), s"unbound arguments $argAttrs in $childOut")
    val names = childOut.map(_.name)
    val types = childOut.map(_.dataType)
    val conf = runnerConf
    val s = spec
    val an = argNames
    val m = metrics
    input() match {
      case Left(rdd) => rdd.mapPartitionsInternal { it =>
        new RowModeRunner(s, names, types, ordinals, an, ColumnarInput(it), conf, m).start()
      }
      case Right(rdd) => rdd.mapPartitionsInternal { it =>
        new RowModeRunner(s, names, types, ordinals, an, RowInput(it), conf, m).start()
      }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): LcPythonEvalExec =
    copy(child = newChild)
}

case class LcPythonAggregateExec(
    specs: Seq[FunctionSpec],
    argAttrs: Seq[Seq[Attribute]],
    argNames: Seq[Seq[String]],
    groupAttrs: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    runnerConf: RunnerConf) extends LcExecBase {

  override def output: Seq[Attribute] = groupAttrs ++ resultAttrs
  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)
  override lazy val references: AttributeSet = AttributeSet(groupAttrs ++ argAttrs.flatten)

  // Every row of a group lands in one partition, sorted so each group is contiguous: the worker
  // then streams group by group, and memory is bounded by the largest group, not by the number
  // of groups in a partition. Spark's Sort spills, so the JVM side stays bounded too.
  override def requiredChildDistribution: Seq[Distribution] =
    if (groupAttrs.isEmpty) AllTuples :: Nil else ClusteredDistribution(groupAttrs) :: Nil
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupAttrs.map(SortOrder(_, Ascending)))
  override def outputPartitioning: Partitioning =
    if (groupAttrs.isEmpty) org.apache.spark.sql.catalyst.plans.physical.SinglePartition
    else child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = groupAttrs.map(SortOrder(_, Ascending))

  override def simpleString(maxFields: Int): String =
    s"LcPythonAggregate [${specs.zip(argAttrs).map { case (s, a) => LcNodes.describe(s, a) }
      .mkString(", ")}] groupBy=[${groupAttrs.mkString(", ")}]"

  override def runColumnar(): RDD[ColumnarBatch] = {
    val childOut = child.output
    def ord(a: Attribute): Int = {
      val i = childOut.indexWhere(_.exprId == a.exprId)
      require(i >= 0, s"unbound attribute $a in $childOut"); i
    }
    val argOrd = argAttrs.map(_.map(ord))
    val keyOrd = groupAttrs.map(ord)
    val types = childOut.map(_.dataType)
    val keyTypes = groupAttrs.map(_.dataType)
    val conf = runnerConf
    val ss = specs
    val an = argNames
    val m = metrics
    val keyExprs = keyOrd.map(o => org.apache.spark.sql.catalyst.expressions.BoundReference(
      o, types(o), nullable = true))
    val mkProj = () => UnsafeProjection.create(keyExprs)
    def run(in: LcInput) = new GroupedRunner(ss, argOrd, an, types, mkProj, keyTypes, in, conf, m).start()
    input() match {
      case Left(rdd) => rdd.mapPartitionsInternal(it => run(ColumnarInput(it)))
      case Right(rdd) => rdd.mapPartitionsInternal(it => run(RowInput(it)))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): LcPythonAggregateExec =
    copy(child = newChild)
}

/**
 * Planner strategy. When Comet's native execution is active, the operator is wrapped in Comet's
 * own `CometSparkToColumnarExec` already here, before Comet's rules run: CometExecRule turns that
 * wrapper into a native input (`CometScanWrapper`), so Comet operators above us stay native
 * instead of falling back to Spark. If nothing Comet-native ends up above, [[LcColumnarRule]]
 * removes the wrapper again.
 */
case class LcStrategy(session: SparkSession) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case e: LcPythonEval =>
      wrap(LcPythonEvalExec(e.spec, e.argAttrs, e.argNames, e.resultAttr, planLater(e.child),
        conf())) :: Nil
    case a: LcPythonAggregate =>
      wrap(LcPythonAggregateExec(a.specs, a.argAttrs, a.argNames, a.groupAttrs, a.resultAttrs,
        planLater(a.child), conf())) :: Nil
    case _ => Nil
  }

  private def conf(): RunnerConf = RunnerConf.fromSession(session.sessionState.conf,
    session.sparkContext.getConf)

  private def wrap(p: SparkPlan): SparkPlan =
    if (CometBridge.nativeOutputEnabled(session)) CometBridge.wrap(p).getOrElse(p) else p
}

/** Reflection-only access to Comet, so this module has no compile-time dependency on it. */
object CometBridge {
  val SparkToColumnar = "org.apache.spark.sql.comet.CometSparkToColumnarExec"
  private lazy val ctor = try {
    Some(Class.forName(SparkToColumnar).getConstructor(classOf[SparkPlan]))
  } catch { case NonFatal(_) => None }

  def available: Boolean = ctor.isDefined

  def nativeOutputEnabled(s: SparkSession): Boolean = {
    val c = s.sessionState.conf
    def b(k: String, d: String) = c.getConfString(k, d).toBoolean
    available && b(LcConf.COMET_NATIVE_OUTPUT, "true") &&
      b("spark.comet.enabled", "false") && b("spark.comet.exec.enabled", "false")
  }

  def wrap(p: SparkPlan): Option[SparkPlan] =
    ctor.flatMap(c => try Some(c.newInstance(p).asInstanceOf[SparkPlan]) catch {
      case NonFatal(_) => None
    })

  def isWrapper(p: SparkPlan): Boolean = p.getClass.getName == SparkToColumnar
  def isComet(p: SparkPlan): Boolean = p.getClass.getName.startsWith("org.apache.spark.sql.comet.")
}

/**
 * Post-transition cleanup around our operators (runs after Spark and Comet inserted transitions):
 *  - below us: drop any row<->columnar transition; we consume either form natively, so Comet
 *    batches reach us untouched and Spark rows are written straight into Arrow.
 *  - above us: drop a Comet wrapper that ended up with no native consumer, and any
 *    columnar->row transition sitting directly on us (we emit rows natively).
 */
case class LcColumnarRule(session: SparkSession) extends ColumnarRule {
  override def postColumnarTransitions: Rule[SparkPlan] = new Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (!plan.exists(_.isInstanceOf[LcExecBase])) return plan
      val pass1 = plan.transformUp {
        case lc: LcExecBase => lc.withNewChildren(Seq(stripBelow(lc.child)))
      }
      val pass2 = pass1.transformUp {
        case t if isC2R(t) && unwrap(t.children.head).isDefined => unwrap(t.children.head).get
        case p if !CometBridge.isComet(p) || isC2R(p) =>
          p.withNewChildren(p.children.map(c =>
            if (CometBridge.isWrapper(c)) unwrap(c).getOrElse(c) else c))
      }
      if (CometBridge.isWrapper(pass2)) unwrap(pass2).getOrElse(pass2) else pass2
    }
  }

  private def isC2R(p: SparkPlan): Boolean =
    p.isInstanceOf[ColumnarToRowTransition] || p.getClass.getSimpleName.endsWith("ColumnarToRowExec")
  private def isR2C(p: SparkPlan): Boolean =
    p.isInstanceOf[RowToColumnarTransition] && !CometBridge.isWrapper(p) ||
      p.getClass.getSimpleName == "RowToColumnarExec"

  private def stripBelow(p: SparkPlan): SparkPlan = p match {
    case t if isC2R(t) || isR2C(t) => stripBelow(t.children.head)
    case w if CometBridge.isWrapper(w) && !w.children.head.isInstanceOf[LcExecBase] =>
      // Comet bridged a non-native Spark source to Arrow for us; read the source directly.
      stripBelow(w.children.head)
    case other => other
  }

  /** Our operator, possibly under a chain of single-child Comet wrapper nodes. */
  private def unwrap(p: SparkPlan): Option[LcExecBase] = p match {
    case lc: LcExecBase => Some(lc)
    case w if CometBridge.isWrapper(w) => unwrap(w.children.head)
    case _ => None
  }
}
