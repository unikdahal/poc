package org.apache.spark.sql.lcpy

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Delegating parser. Spark's grammar already accepts the syntax (`<mod>.<fn>(...)` is a
 * qualified function call), so instead of forking the grammar we post-process the delegate's
 * tree, turning `lc_python_runner(...)` calls into [[UnresolvedLcCall]]s. Works for SQL text,
 * `expr()` / `selectExpr()` and persisted view text alike.
 */
class LcParser(delegate: ParserInterface) extends ParserInterface {

  override def parsePlan(sqlText: String): LogicalPlan = LcParser.rewritePlan(delegate.parsePlan(sqlText))
  override def parseQuery(sqlText: String): LogicalPlan = LcParser.rewritePlan(delegate.parseQuery(sqlText))
  override def parseExpression(sqlText: String): Expression =
    LcParser.rewriteExpr(delegate.parseExpression(sqlText))

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)
  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)
  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)
  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)
}

object LcParser {
  val NAME = "lc_python_runner"

  def rewritePlan(plan: LogicalPlan): LogicalPlan =
    plan.transformUpWithSubqueries { case p => p.transformExpressionsUp(rewrite) }

  def rewriteExpr(e: Expression): Expression = e.transformUp(rewrite)

  private val rewrite: PartialFunction[Expression, Expression] = {
    case u: UnresolvedFunction if u.nameParts.size == 1 && u.nameParts.head.equalsIgnoreCase(NAME) =>
      if (u.isDistinct || u.filter.isDefined || u.ignoreNulls) {
        throw new AnalysisException(
          s"$NAME does not accept DISTINCT, FILTER or IGNORE NULLS")
      }
      u.arguments match {
        case (inner: UnresolvedFunction) +: groupBy
            if inner.nameParts.size == 2 && !inner.isDistinct && inner.filter.isEmpty =>
          UnresolvedLcCall(inner.nameParts.head, inner.nameParts(1), inner.arguments, groupBy)
        case (inner: UnresolvedFunction) +: _ =>
          throw new AnalysisException(
            s"$NAME: the first argument must be <module>.<function>(args...), got " +
              s"'${inner.nameParts.mkString(".")}(...)'")
        case _ =>
          throw new AnalysisException(
            s"$NAME: the first argument must be <module>.<function>(args...)")
      }
  }
}
