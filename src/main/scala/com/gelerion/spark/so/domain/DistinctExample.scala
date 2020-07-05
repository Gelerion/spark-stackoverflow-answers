package com.gelerion.spark.so.domain

import com.gelerion.spark.Spark
import org.apache.spark.sql.functions._

object DistinctExample extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val df = Seq(
      Country("England", 1, 1),
      Country("England", 2, 2),
      Country("England", 2, 2),
      Country("England", 3, 3),
      Country("Russia", 1, 1),
      Country("Russia", 2, 2)
    ) .toDF()

    df.createOrReplaceTempView("dataset")

    val groupDistinct = spark.sql(
      """
        |select country, sum(product_id), count(distinct user_id)
        |from dataset
        |group by country
        |""".stripMargin)

    groupDistinct.explain(true)
    groupDistinct.show(false)
  }

}

case class Country(country: String, product_id: Int, user_id: Int)
