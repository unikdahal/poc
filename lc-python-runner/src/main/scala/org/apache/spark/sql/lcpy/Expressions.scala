package org.apache.spark.sql.lcpy

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.{Expression, Unevaluable}
import org.apache.spark.sql.catalyst.trees.TreePattern.{TreePattern, UNRESOLVED_FUNCTION}
import org.apache.spark.sql.types.DataType

/**
 * Parser output for `lc_python_runner(<mod>.<fn>(args...), group_by...)`.
 *
 * The parser rewrites the generic `UnresolvedFunction("lc_python_runner", ...)` into this node so
 * that Spark's function resolution never tries to look up `<mod>.<fn>` in the catalog (it would
 * fail with UNRESOLVED_ROUTINE). Its children are ordinary expressions that the analyzer resolves
 * as usual; [[ResolveLcCalls]] then binds it to the registry.
 */
case class UnresolvedLcCall(
    module: String,
    function: String,
    args: Seq[Expression],
    groupBy: Seq[Expression])
  extends Expression with Unevaluable {

  override def children: Seq[Expression] = args ++ groupBy
  override lazy val resolved: Boolean = false
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  // Keeps Spark's generic "is there anything left to resolve" checks honest.
  override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_FUNCTION)

  override def prettyName: String = "lc_python_runner"
  override def sql: String = LcCall.renderSql(module, function, args, groupBy)
  override def toString: String = sql

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(args = newChildren.take(args.size), groupBy = newChildren.drop(args.size))
}

/**
 * A resolved invocation. Exists only between analysis-time resolution and the post-hoc extraction
 * rule ([[ExtractLcCalls]]), which replaces every instance with an attribute produced by an
 * [[LcPythonEval]] / [[LcPythonAggregate]] node. It is never evaluated.
 *
 * `implicitArgs` is true when the user wrote `<mod>.<fn>()`: the function then receives every
 * column of the child relation (bound during extraction, when the child is known).
 */
case class LcCall(
    spec: FunctionSpec,
    args: Seq[Expression],
    groupBy: Seq[Expression],
    implicitArgs: Boolean)
  extends Expression with Unevaluable {

  override def children: Seq[Expression] = args ++ groupBy
  override def dataType: DataType = spec.returnType
  override def nullable: Boolean = true
  // The Python code is opaque: never let generic rules treat it as deterministic.
  override lazy val deterministic: Boolean = false

  override def prettyName: String = "lc_python_runner"
  override def sql: String = LcCall.renderSql(spec.module, spec.function, args, groupBy)
  override def toString: String = sql

  def mode: Mode = spec.mode
  def isGrouped: Boolean = spec.mode == Mode.Grouped

  /**
   * Identity used for redundant-call elimination. `canonicalized` already normalises exprIds and
   * cosmetic differences of the children; spec equality covers module, function and code hash.
   * (We cannot use `semanticEquals`, which requires determinism.)
   */
  lazy val dedupKey: (FunctionSpec, Seq[Expression], Seq[Expression], Boolean) =
    (spec, args.map(_.canonicalized), groupBy.map(_.canonicalized), implicitArgs)

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(args = newChildren.take(args.size), groupBy = newChildren.drop(args.size))
}

object LcCall {
  def renderSql(m: String, f: String, args: Seq[Expression], groupBy: Seq[Expression]): String = {
    val inner = s"$m.$f(${args.map(_.sql).mkString(", ")})"
    (inner +: groupBy.map(_.sql)).mkString("lc_python_runner(", ", ", ")")
  }

  def contains(e: Expression): Boolean = e.find {
    case _: LcCall | _: UnresolvedLcCall => true
    case _ => false
  }.isDefined

  /** Calls that do not themselves contain another call: the next level to extract. */
  def innermost(exprs: Seq[Expression]): Seq[LcCall] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[LcCall]
    def visit(e: Expression): Unit = e match {
      case c: LcCall if !c.children.exists(contains) => out += c
      case other => other.children.foreach(visit)
    }
    exprs.foreach(visit)
    out.toSeq
  }

  def all(exprs: Seq[Expression]): Seq[LcCall] =
    exprs.flatMap(_.collect { case c: LcCall => c })
}
