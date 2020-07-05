package com.gelerion.spark.skew.nulls.hints

import java.util.Locale

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.ResolveHints.ResolveCoalesceHints.COALESCE_HINT_NAMES
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule

object ResolveSkewedHint {
//  extends Rule[LogicalPlan] {
//  private val SKEWED_HINT_NAMES = Set("skewed")
//
//  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
//    case h: UnresolvedHint if SKEWED_HINT_NAMES.contains(h.name.toUpperCase(Locale.ROOT)) =>
//      val hintName = h.name.toUpperCase(Locale.ROOT)
//      val shuffle = hintName match {
//        case "REPARTITION" => true
//        case "COALESCE" => false
//      }
//      val numPartitions = h.parameters match {
//        case Seq(IntegerLiteral(numPartitions)) =>
//          numPartitions
//        case Seq(numPartitions: Int) =>
//          numPartitions
//        case _ =>
//          throw new AnalysisException(s"$hintName Hint expects a partition number as parameter")
//      }
//      Repartition(numPartitions, shuffle, h.child)
//  }

}
