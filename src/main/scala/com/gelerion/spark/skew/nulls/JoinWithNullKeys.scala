package com.gelerion.spark.skew.nulls

import com.gelerion.spark.Spark
import com.gelerion.spark.skew.{Country, Location, Region}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

object JoinWithNullKeys extends Spark {
  import spark.implicits._

  //https://stackoverflow.com/questions/52339872/spark-dataset-dataframe-join-null-skew-key?rq=1
  //https://stackoverflow.com/questions/57797559/spark-sql-1-task-running-for-long-time-due-to-null-values-is-join-key
  def main(args: Array[String]): Unit = {
    val countries: DataFrame = List(
      Country("CN", "China"),
      Country("UK", "United Kingdom"),
      Country("US", "United States of America"),
      Country(null, "Unknown 1"),
      Country(null, "Unknown 2"),
      Country(null, "Unknown 3"),
      Country(null, "Unknown 4"),
      Country(null, "Unknown 5"),
      Country(null, "Unknown 6")
    ).toDF()


    val locations = List(
      Location(1400, "2014 Jabberwocky Rd", "Southlake", "US"),
      Location(1500, "2011 Interiors Blvd", "San Francisco", "US"),
      Location(1700, "2004 Charade Rd", "Seattle", "US"),
      Location(2400, "8204 Arthur St", "London", "UK"),
      Location(2500, "Magdalen Centre, The Oxford Science Park", "Oxford", "UK"),
      Location(0, "Null Street", "Null City", null),
    ).toDF()

    import com.gelerion.spark.skew.nulls.solutiion.replace.ReplaceNullKeys.SkewedDataFrameExt


    //As is org.apache.spark.sql.AnalysisException: Reference 'country_id' is ambiguous, could be: country_id, country_id.;
    //In order to resolve ambiguoty
    val skewedSafeJoin = countries
      .nullSkewLeftJoin(locations, "country_id")


    skewedSafeJoin.explain(true)
    skewedSafeJoin.show(false)

    countries.join(locations, Seq("country_id"), "left")

  }

}
