package org.apache.spark.sql.lcpy

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.apache.spark.sql.util.ArrowUtils

private[lcpy] object LcErrors {
  def analysis(msg: String): AnalysisException = new AnalysisException(s"lc_python_runner: $msg")
}

/**
 * Resolution-batch rule: binds [[UnresolvedLcCall]] to the registry once its children are
 * resolved, and rejects argument / grouping shapes we do not support. Everything here runs during
 * analysis, so bad queries fail before any job is submitted.
 */
case class ResolveLcCalls(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case p => p.transformExpressionsUp {
      case u: UnresolvedLcCall if u.children.forall(_.resolved) => resolve(u)
    }
  }

  private def resolve(u: UnresolvedLcCall): LcCall = {
    val dir = LcConf.sqlConf(LcConf.REGISTRY_DIR,
      session.sparkContext.getConf.get(LcConf.REGISTRY_DIR, ""))
    val spec = Registry.lookup(dir, u.module, u.function)
    val what = s"${spec.qualifiedName}"

    if (spec.mode == Mode.Row && u.groupBy.nonEmpty) {
      throw LcErrors.analysis(s"$what is registered as row-preserving (1-to-1) and does not " +
        s"accept <group_by> expressions (got ${u.groupBy.map(_.sql).mkString(", ")})")
    }
    def reject(e: Expression, where: String): Unit = e.foreach {
      case g: Generator => throw LcErrors.analysis(
        s"table-generating function ${g.prettyName} is not allowed in $where of $what")
      case a @ (_: AggregateExpression | _: AggregateFunction) => throw LcErrors.analysis(
        s"aggregate function ${a.prettyName} is not allowed in $where of $what; " +
          "compute it in a subquery first")
      case w: WindowExpression => throw LcErrors.analysis(
        s"window function ${w.sql} is not allowed in $where of $what")
      case _ =>
    }
    u.args.foreach(reject(_, "the arguments"))
    u.groupBy.foreach { g =>
      reject(g, "<group_by>")
      if (g.find { case c: LcCall => c.isGrouped; case _ => false }.isDefined) {
        throw LcErrors.analysis(s"<group_by> of $what contains an N-to-1 lc_python_runner call")
      }
      if (!g.deterministic && !LcCall.contains(g)) {
        throw LcErrors.analysis(s"<group_by> expression ${g.sql} of $what is not deterministic")
      }
      if (!RowOrdering.isOrderable(g.dataType)) {
        throw LcErrors.analysis(
          s"<group_by> expression ${g.sql} of $what has non-orderable type ${g.dataType.sql}")
      }
    }
    u.args.foreach { a =>
      try ArrowUtils.toArrowField("a", a.dataType, a.nullable, "UTC")
      catch {
        case ex: Exception => throw LcErrors.analysis(
          s"argument ${a.sql} of $what has type ${a.dataType.sql}, which cannot be sent " +
            s"to Python as Arrow (${ex.getMessage})")
      }
    }
    LcCall(spec, u.args, u.groupBy, implicitArgs = u.args.isEmpty)
  }
}

/**
 * Post-hoc rule: removes every [[LcCall]] from the plan, replacing it with an attribute produced
 * by a dedicated operator.
 *
 *  - Innermost calls are extracted first, so `lc(m.outer(lc(m.inner(a)).x + 1))` becomes
 *    `LcPythonEval(outer, Project(.. x + 1 .., LcPythonEval(inner, ..)))`, to any depth.
 *  - Calls with the same function, code hash and (canonicalized) inputs are evaluated once per
 *    scope, including across a Filter and the Project above it.
 *  - N-to-1 calls turn the projection that contains them into an aggregation: everything else in
 *    the SELECT list may only reference the call results and its <group_by> expressions.
 *
 * Runs at analysis time (not in the optimizer) because an N-to-1 call changes cardinality: left
 * as an expression inside a Project, optimizer rules such as column pruning would treat it as
 * row-preserving and silently change results.
 */
object ExtractLcCalls extends Rule[LogicalPlan] {

  private val DEDUP_KEY = TreeNodeTag[Any]("lcpy.dedupKey")
  private val RESTORE = TreeNodeTag[Boolean]("lcpy.restore")
  val INTERNAL_KEY = "__lcpy_internal"
  private val internalMeta: Metadata = new MetadataBuilder().putBoolean(INTERNAL_KEY, true).build()

  def isInternal(a: Attribute): Boolean =
    a.metadata.contains(INTERNAL_KEY) && a.metadata.getBoolean(INTERNAL_KEY)

  private def hasCall(es: Seq[Expression]): Boolean = es.exists(LcCall.contains)

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case p: Project if hasCall(p.projectList) => rewriteProject(p)
    case f: Filter if hasCall(f.condition :: Nil) => rewriteFilter(f)
    case s: Sort if hasCall(s.order) => rewriteSort(s)
    case a: Aggregate if hasCall(a.groupingExpressions ++ a.aggregateExpressions) =>
      rewriteAggregate(a)
    case other if hasCall(other.expressions) =>
      throw LcErrors.analysis(s"calls are not supported inside ${other.nodeName} " +
        "(supported: SELECT list, WHERE, GROUP BY / aggregate function inputs, ORDER BY); " +
        "compute the call in a subquery and reference its column instead")
  }

  // ----------------------------------------------------------------------------------- operators

  private def unwrapRestore(child: LogicalPlan): LogicalPlan = child match {
    case p: Project if p.getTagValue(RESTORE).contains(true) => p.child
    case other => other
  }

  private def restore(output: Seq[Attribute], plan: LogicalPlan): LogicalPlan = {
    val p = Project(output, plan)
    p.setTagValue(RESTORE, true)
    p
  }

  private def rewriteProject(p: Project): LogicalPlan = {
    val child = unwrapRestore(p.child)
    val (es, newChild) = extract(p.projectList, child, allowGrouped = true, "SELECT")
    Project(es.map(_.asInstanceOf[NamedExpression]), newChild)
  }

  private def rewriteFilter(f: Filter): LogicalPlan = {
    val (Seq(cond), newChild) = extract(f.condition :: Nil, f.child, allowGrouped = false, "WHERE")
    restore(f.output, Filter(cond, newChild))
  }

  private def rewriteSort(s: Sort): LogicalPlan = {
    val (order, newChild) = extract(s.order, s.child, allowGrouped = false, "ORDER BY")
    restore(s.output, s.copy(order = order.map(_.asInstanceOf[SortOrder]), child = newChild))
  }

  private def rewriteAggregate(a: Aggregate): LogicalPlan = {
    LcCall.all(a.groupingExpressions ++ a.aggregateExpressions).find(_.isGrouped).foreach { c =>
      throw LcErrors.analysis(s"N-to-1 call ${c.spec.qualifiedName} cannot be used inside a " +
        "GROUP BY query; it is itself an aggregation. Use it in a SELECT without GROUP BY " +
        "(passing the keys as its <group_by>), or aggregate in a subquery")
    }
    // Calls in GROUP BY keys and inside aggregate-function inputs are per input row: evaluate
    // them below the Aggregate. Collect the expressions that hold them and extract jointly so
    // that the same call in the SELECT list and in GROUP BY is evaluated once.
    val holders = mutable.ArrayBuffer.empty[Expression]
    holders ++= a.groupingExpressions
    a.aggregateExpressions.foreach(_.foreach {
      case ae: AggregateExpression if LcCall.contains(ae) => holders += ae
      case _ =>
    })
    val (rewritten, newChild) = extract(holders.toSeq, a.child, allowGrouped = false, "GROUP BY")
    val mapping = holders.zip(rewritten).toMap
    val calls = LcCall.all(holders.toSeq)
    val byKey: Map[Any, Expression] = {
      // Every extracted call maps to the attribute (or attribute-derived expression) that replaced
      // it; reuse that for the same call appearing elsewhere in the SELECT list.
      val avail = available(newChild)
      calls.flatMap(c => avail.get(c.dedupKey).map(attr => (c.dedupKey: Any) -> (attr: Expression)))
        .toMap
    }
    def subst(e: Expression): Expression = e.transformDown {
      case h if mapping.contains(h) => mapping(h)
      case c: LcCall if byKey.contains(c.dedupKey) => byKey(c.dedupKey)
    }
    val newGrouping = a.groupingExpressions.map(subst)
    val newAggs = a.aggregateExpressions.map(subst(_).asInstanceOf[NamedExpression])
    if (hasCall(newAggs)) {
      throw LcErrors.analysis("calls over aggregated values or GROUP BY keys in the SELECT list " +
        "of a GROUP BY query are not supported; aggregate in a subquery and apply " +
        "lc_python_runner in the outer SELECT")
    }
    Aggregate(newGrouping, newAggs, newChild)
  }

  // --------------------------------------------------------------------------------- extraction

  /** Calls already materialised below `child` whose result column is still visible. */
  private def available(child: LogicalPlan): Map[Any, Attribute] = {
    val out = child.outputSet
    child.collect {
      case e: LcPythonEval if e.getTagValue(DEDUP_KEY).isDefined && out.contains(e.resultAttr) =>
        e.getTagValue(DEDUP_KEY).get -> e.resultAttr
    }.toMap
  }

  private case class Found(call: LcCall, insideGrouped: Boolean)

  /** Innermost calls, each flagged with whether it sits inside an N-to-1 call's arguments. */
  private def innermost(es: Seq[Expression]): Seq[Found] = {
    val out = mutable.ArrayBuffer.empty[Found]
    def visit(e: Expression, inG: Boolean): Unit = e match {
      case c: LcCall if !c.children.exists(LcCall.contains) => out += Found(c, inG)
      case c: LcCall => c.children.foreach(visit(_, inG || c.isGrouped))
      case other => other.children.foreach(visit(_, inG))
    }
    es.foreach(visit(_, inG = false))
    out.toSeq
  }

  private def distinctByKey(cs: Seq[LcCall]): Seq[LcCall] = {
    val seen = mutable.LinkedHashMap.empty[Any, LcCall]
    cs.foreach(c => seen.getOrElseUpdate(c.dedupKey, c))
    seen.values.toSeq
  }

  private def replaceCalls(es: Seq[Expression], m: Map[Any, Expression]): Seq[Expression] =
    es.map(_.transformUp { case c: LcCall if m.contains(c.dedupKey) => m(c.dedupKey) })

  private def extract(
      exprs: Seq[Expression],
      child0: LogicalPlan,
      allowGrouped: Boolean,
      clause: String): (Seq[Expression], LogicalPlan) = {
    var es = exprs
    var child = child0
    var aggregated: Option[LcPythonAggregate] = None

    while (hasCall(es)) {
      val level = innermost(es)
      val anyGroupedPending = LcCall.all(es).exists(_.isGrouped)
      if (anyGroupedPending && !allowGrouped) {
        val g = LcCall.all(es).find(_.isGrouped).get
        throw LcErrors.analysis(s"N-to-1 call ${g.spec.qualifiedName} is not allowed in $clause " +
          "(it changes cardinality); call it in a SELECT list, in a subquery if needed")
      }
      if (anyGroupedPending && aggregated.isDefined) {
        throw LcErrors.analysis("an N-to-1 call over the result of another N-to-1 call in the " +
          "same SELECT is not supported; aggregate in a subquery first")
      }

      // While an N-to-1 call is still pending, only calls feeding it may be evaluated below it;
      // other row calls must wait until after the aggregation (they see the grouped rows).
      val rowCalls = level.collect {
        case Found(c, inG) if !c.isGrouped && (!anyGroupedPending || inG) => c
      }
      val groupedCalls = level.collect { case Found(c, _) if c.isGrouped => c }

      if (rowCalls.nonEmpty) {
        val avail = available(child)
        val subst = mutable.Map.empty[Any, Expression]
        distinctByKey(rowCalls).foreach { c =>
          val reuse = if (c.implicitArgs) None else avail.get(c.dedupKey)
          val attr = reuse.getOrElse {
            val node = buildEval(c, child, aggregated.isDefined)
            child = node
            node.resultAttr
          }
          subst(c.dedupKey) = attr
        }
        es = replaceCalls(es, subst.toMap)
      } else if (groupedCalls.nonEmpty) {
        val (node, newEs) = buildAggregate(distinctByKey(groupedCalls), es, child)
        es = newEs
        child = node
        aggregated = Some(node)
      } else {
        throw new IllegalStateException(s"no extractable call in ${es.map(_.sql)}")
      }
    }

    aggregated.foreach { agg =>
      val missing = es.flatMap(_.references).filterNot(child.outputSet.contains).distinct
      if (missing.nonEmpty) {
        throw ungrouped(agg, missing)
      }
    }
    (es, child)
  }

  private def ungrouped(agg: LcPythonAggregate, missing: Seq[Attribute]): AnalysisException = {
    val fns = agg.specs.map(_.qualifiedName).mkString(", ")
    LcErrors.analysis(s"column(s) ${missing.map(a => s"`${a.name}`").distinct.mkString(", ")} " +
      s"referenced alongside the N-to-1 call(s) $fns are neither part of its <group_by> " +
      "(the SELECT list of an N-to-1 projection may only reference <group_by> expressions and " +
      "the call results)")
  }

  private def argNames(args: Seq[Expression]): Seq[String] = {
    val raw = args.zipWithIndex.map {
      case (a: Attribute, _) => a.name
      case (Alias(_, n), _) => n
      case (_, i) => s"arg$i"
    }
    val seen = mutable.Map.empty[String, Int]
    raw.map { n =>
      val k = seen.getOrElse(n, 0)
      seen(n) = k + 1
      if (k == 0) n else s"${n}_$k"
    }
  }

  /** Project computing non-attribute expressions as internal columns, keeping child.output. */
  private def bindToAttributes(
      exprs: Seq[Expression],
      child: LogicalPlan,
      prefix: String): (Seq[Attribute], LogicalPlan) = {
    val extra = mutable.ArrayBuffer.empty[Alias]
    val attrs = exprs.map {
      case a: Attribute if child.outputSet.contains(a) => a
      case e =>
        val al = Alias(e, s"__lc_$prefix${extra.size}")(explicitMetadata = Some(internalMeta))
        extra += al
        al.toAttribute
    }
    val plan = if (extra.isEmpty) child else Project(child.output ++ extra, child)
    (attrs, plan)
  }

  private def checkInputs(c: LcCall, child: LogicalPlan, afterAgg: Boolean): Unit = {
    val missing = c.references.filterNot(child.outputSet.contains).toSeq
    if (missing.nonEmpty) {
      if (afterAgg) {
        throw LcErrors.analysis(s"call ${c.spec.qualifiedName} references column(s) " +
          s"${missing.map(a => s"`${a.name}`").mkString(", ")} that are not available after " +
          "the N-to-1 aggregation in the same SELECT (only <group_by> expressions and call " +
          "results are)")
      }
      throw new IllegalStateException(
        s"lc_python_runner: unresolvable inputs ${missing.mkString(", ")} for ${c.sql}")
    }
  }

  private def buildEval(c: LcCall, child: LogicalPlan, afterAgg: Boolean): LcPythonEval = {
    checkInputs(c, child, afterAgg)
    val args = if (c.implicitArgs) child.output.filterNot(isInternal) else c.args
    val (argAttrs, pre) = bindToAttributes(args, child, "arg")
    val result = AttributeReference(s"__lc_${c.spec.function}", c.spec.returnType,
      nullable = true, internalMeta)()
    val node = LcPythonEval(c.spec, argAttrs, argNames(args), result, pre)
    node.setTagValue(DEDUP_KEY, c.dedupKey)
    node
  }

  private def buildAggregate(
      calls: Seq[LcCall],
      es: Seq[Expression],
      child: LogicalPlan): (LcPythonAggregate, Seq[Expression]) = {
    val keySets = calls.map(c => c.groupBy.map(_.canonicalized))
    if (keySets.distinct.size > 1) {
      throw LcErrors.analysis("N-to-1 calls in the same SELECT must use identical <group_by> " +
        "keys; got " + calls.map(c => s"${c.spec.qualifiedName} BY " +
        s"(${c.groupBy.map(_.sql).mkString(", ")})").mkString(" vs "))
    }
    calls.foreach(c => checkInputs(c, child, afterAgg = false))
    val groupExprs = calls.head.groupBy
    // Grouping keys and all arguments are bound in one Project below the aggregation.
    val allArgs = calls.map(c => if (c.implicitArgs) child.output.filterNot(isInternal) else c.args)
    val flat = groupExprs ++ allArgs.flatten
    val (flatAttrs, pre) = bindToAttributes(flat, child, "g")
    val groupAttrs = flatAttrs.take(groupExprs.size)
    var rest = flatAttrs.drop(groupExprs.size)
    val argAttrs = allArgs.map { a => val (h, t) = rest.splitAt(a.size); rest = t; h }
    val results = calls.map(c => AttributeReference(s"__lc_${c.spec.function}",
      c.spec.returnType, nullable = true, internalMeta)())
    val node = LcPythonAggregate(calls.map(_.spec), argAttrs, allArgs.map(argNames),
      groupAttrs, results, pre)

    val callSubst: Map[Any, Expression] = calls.zip(results).map { case (c, r) => c.dedupKey -> r }.toMap
    val keySubst: Map[Expression, Attribute] = groupExprs.map(_.canonicalized).zip(groupAttrs).toMap
    val newEs = replaceCalls(es, callSubst).map(_.transformDown {
      case e if !e.isInstanceOf[Attribute] && !e.isInstanceOf[Literal] &&
        keySubst.contains(e.canonicalized) => keySubst(e.canonicalized)
    })
    (node, newEs)
  }
}

/**
 * Optimizer rule: push deterministic filter conjuncts that only touch pass-through columns below
 * the Python operator, so the sandbox never sees rows that are filtered out anyway. Correct for
 * 1-to-1 functions, which are row-local by contract.
 */
object PushFiltersBelowLcEval extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!LcConf.sqlConf(LcConf.PUSH_FILTERS, "true").toBoolean) return plan
    plan.transformUp {
      case f @ Filter(cond, e: LcPythonEval) =>
        val (pushable, stay) = splitConjunctivePredicates(cond).partition { c =>
          c.deterministic && !c.references.contains(e.resultAttr)
        }
        if (pushable.isEmpty) f
        else {
          val pushed = e.copy(child = Filter(pushable.reduce(And), e.child))
          if (stay.isEmpty) pushed else Filter(stay.reduce(And), pushed)
        }
    }
  }

  private def splitConjunctivePredicates(e: Expression): Seq[Expression] = e match {
    case And(l, r) => splitConjunctivePredicates(l) ++ splitConjunctivePredicates(r)
    case other => other :: Nil
  }
}
