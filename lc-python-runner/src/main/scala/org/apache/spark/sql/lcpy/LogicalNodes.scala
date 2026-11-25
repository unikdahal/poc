package org.apache.spark.sql.lcpy

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

/**
 * Attributes are kept as direct constructor fields (not wrapped in a helper case class) so that
 * Catalyst's generic `transformExpressions` sees and rewrites them, e.g. when an optimizer rule
 * renumbers exprIds.
 */
object LcNodes {
  def describe(spec: FunctionSpec, args: Seq[Attribute]): String =
    s"${spec.qualifiedName}(${args.map(_.name).mkString(", ")})"
}

/**
 * 1-to-1 evaluation: output = every child column, unchanged and row-aligned, plus one struct
 * column. Arguments are plain attributes; arbitrary argument expressions were computed by a
 * Project below (which Comet can run natively).
 */
case class LcPythonEval(
    spec: FunctionSpec,
    argAttrs: Seq[Attribute],
    argNames: Seq[String],
    resultAttr: Attribute,
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output :+ resultAttr
  override def producedAttributes: AttributeSet = AttributeSet(resultAttr :: Nil)
  override lazy val references: AttributeSet = AttributeSet(argAttrs)

  override def simpleString(maxFields: Int): String =
    s"LcPythonEval ${LcNodes.describe(spec, argAttrs)} -> $resultAttr"

  override protected def withNewChildInternal(newChild: LogicalPlan): LcPythonEval =
    copy(child = newChild)
}

/**
 * N-to-1 evaluation: one output row per distinct `groupAttrs` value (exactly one row when
 * `groupAttrs` is empty), carrying the grouping columns plus one struct per call. Several calls
 * in one projection share the node (and one worker) iff their grouping keys are identical.
 */
case class LcPythonAggregate(
    specs: Seq[FunctionSpec],
    argAttrs: Seq[Seq[Attribute]],
    argNames: Seq[Seq[String]],
    groupAttrs: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = groupAttrs ++ resultAttrs
  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)
  override lazy val references: AttributeSet = AttributeSet(groupAttrs ++ argAttrs.flatten)

  override def simpleString(maxFields: Int): String =
    s"LcPythonAggregate [${specs.zip(argAttrs).map { case (s, a) => LcNodes.describe(s, a) }
      .mkString(", ")}] groupBy=[${groupAttrs.mkString(", ")}] -> [${resultAttrs.mkString(", ")}]"

  override protected def withNewChildInternal(newChild: LogicalPlan): LcPythonAggregate =
    copy(child = newChild)
}
