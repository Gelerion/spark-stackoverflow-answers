package com.gelerion.spark.so.joins

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ShuffleHashJoin {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("ShuffleHashJoin")
      .master("local[*]")
      .getOrCreate()

    /*
    * Disable auto broadcasting of table and SortMergeJoin
    */
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 2)
    spark.conf.set("spark.sql.join.preferSortMergeJoin", false)

    import spark.implicits._
    val dataset = Seq(
      (0, "playing"),
      (1, "with"),
      (2, "ShuffledHashJoinExec")).toDF("id", "token")

    val right = Seq(
      (0, "asdfghjklzxcvb"),
      (1, "asdfghjklzxcvb"),
      (2, "asdfghjklzxcvb"),
      (3, "asdfghjklzxcvb"),
      (4, "asdfghjklzxcvb"),
      (5, "asdfghjklzxcvb"),
      (6, "asdfghjklzxcvb"),
      (7, "asdfghjklzxcvb"),
      (8, "asdfghjklzxcvb"),
      (9, "asdfghjklzxcvb"),
    )
      .toDF("id", "token")

    val joined = dataset.join(right, Seq("id"), "inner")
    joined.explain(true)
    joined.foreach(_ => ())

    // infinite loop to keep the program running to check Spark UI at 4040 port.
    //while (true) {}
  }
}
