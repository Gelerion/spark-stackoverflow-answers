package com.gelerion.spark.so.joins

import com.gelerion.spark.Spark
import com.gelerion.spark.so.general.GeneralAnswers.spark
import org.apache.spark.sql.functions._

object Joins extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df1 = Seq((1, "a"), (1, "b"), (1, "c"), (1, "d"), (1, "e")).toDF("id", "word")
    val df2 = Seq((1, 1), (1, 2), (1, 3), (1, 4), (1, 5)).toDF("id", "idx")

    df1.join(df2, df1("id") === df2("id")).show() //joinExpr to eliminate duplicates

  }


}
