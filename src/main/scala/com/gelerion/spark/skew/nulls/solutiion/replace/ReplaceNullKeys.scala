package com.gelerion.spark.skew.nulls.solutiion.replace

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object ReplaceNullKeys {

  /**
   * Expression that produce negative random between -1 and -`lowestValue`(inclusively).
   *
   * @example
   * {{{
   *             spark
   *                  .range(1, 100)
   *                  .withColumn("negative", negativeRandomWithin(3))
   *                  .select("negative")
   *                  .distinct()
   *                  .show(false)
   * }}}
   *          +--------+
   *          |negative|
   *          +--------+
   *          | -2     |
   *          | -3     |
   *          | -1     |
   *          +--------+
   */
  private def negativeRandomWithin(lowestValue: Long): Column = {
    negate(positiveRandomWithin(lowestValue)) - 1
  }

  /**
   * Expression that produce positive random between 0 and `highestValue`(exclusively).
   *
   * @example
   *          {{{
   *             spark
   *                  .range(1, 100)
   *                  .withColumn("positive", positiveRandomWithin(3))
   *                  .select("positive")
   *                  .distinct()
   *                  .show(false)
   * }}}
   *          +--------+
   *          |positive|
   *          +--------+
   *          |0       |
   *          |1       |
   *          |2       |
   *          +--------+
   */
  private def positiveRandomWithin(highestValue: Long) = {
    pmod((rand * highestValue).cast(LongType), lit(highestValue))
  }

  implicit class SkewedDataFrameExt(val underlying: DataFrame) extends AnyVal {

    /**
     * Particular optimized version of left outer join where left side of join has skewed `null` field.
     *
     * @note
     *       It works only for single column join which is applicable for `isNotNull`.
     *
     *       Optimization algorithm:
     *   1. replace left dataset `null` values with negative number within range between -1 and - `nullNumBuckets`(10000 by default)
     *   2. use appended column, with original join column value and `null` replacements, as join column from left dataset
     *       appended column name builds using original left join column and `skewedColumnPostFix` separated by underscore.
     * @note there is no checks how many `null` values on left dataset before applying above steps,
     *       as well as there is no checks does it sort merge join or broadcast.
     *
     *       IMPORTANT: If left dataset already has appended column name, it will be reused to benefit already repartitioned data on the left
     *
     *       HIGHLY IMPORTANT: right dataset should not contain negative values in `joinRightCol`
     */
    def nullSkewLeftJoin(right: DataFrame,
                         usingColumn: String,
                         skewedColumnPostFix: String = "skewed_column",
                         nullNumBuckets: Int = 10000): DataFrame = {
      val left = underlying

      val leftColumn = left.col(usingColumn)
      val rightColumn = right.col(usingColumn)

      nullSkewLeftJoin(right, leftColumn, rightColumn, skewedColumnPostFix, nullNumBuckets)
    }

    def nullSkewLeftJoin(right: DataFrame,
                         joinLeftCol: Column,
                         joinRightCol: Column,
                         skewedColumnPostFix: String ,
                         nullNumBuckets: Int): DataFrame = {

      val skewedTempColumn = s"${joinLeftCol.toString()}_$skewedColumnPostFix"

      if (underlying.columns.exists(_ equalsIgnoreCase skewedTempColumn)) {
        underlying.join(right.where(joinRightCol.isNotNull), col(skewedTempColumn) === joinRightCol, "left")
      } else {
        underlying
          .withColumn(skewedTempColumn,
            when(joinLeftCol.isNotNull, joinLeftCol).otherwise(negativeRandomWithin(nullNumBuckets)))
          .join(right.where(joinRightCol.isNotNull), col(skewedTempColumn) === joinRightCol, "left")
      }
    }
  }
}


