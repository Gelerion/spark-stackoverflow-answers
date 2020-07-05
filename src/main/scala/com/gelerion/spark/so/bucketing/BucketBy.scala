package com.gelerion.spark.so.bucketing

import com.gelerion.spark.Spark
import com.gelerion.spark.so.general.GeneralAnswers.spark
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions._

object BucketBy extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //bucketByCustom()

    bucketByDaysInMonth()
  }

  //https://stackoverflow.com/questions/58006110/spark-scala-how-to-save-a-grouped-dataframe-to-different-files
  def bucketByDaysInMonth(): Unit = {
    val df = Seq(
      (1, "data1"),
      (1, "data1_1"),
      (2, "data2_1"),
      (3, "data3_1"),
      (4, "data4_1"),
      (4, "data4_2"),
      (4, "data4_3")
    ).toDF("day", "data")

    df.write.option("header", true)
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .csv("report")

  }

  //Is there anyway I can split the dataframe into 4 different partitions and write each partition as a files based on the column ID
  def bucketByCustom(): Unit = {
    val data = Seq(
      ("db2", 10, "Hive"),
      ("sap", 20, "Hive"),
      ("sql", 17, "Hive"),
      ("oracle", 21, "Hive"),
      ("server", 33, "Hive"),
      ("risk", 43, "Hive"),
    ).toDF("source_name", "id", "location")

    val bucketed = data.withColumn("bucket",
         when($"id".between(0, 10), "1-10")
         .when($"id".between(11, 20), "11-20")
         .when($"id".between(21, 30), "21-30")
         .when($"id".between(31, 40), "31-40")
         .when($"id".between(41, 50), "41-10")
         .otherwise("50+"))


    /*
    df.withColumn("new_gender",
      expr("case when gender = 'M' then 'Male' " +
            "when gender = 'F' then 'Female' " +
            "else 'Unknown' end"))
     */

    bucketed.write.option("header", true)
      .mode(SaveMode.Overwrite)
      .partitionBy("bucket")
      //.bucketBy(4, "id")
      .csv("bucketing")
  }

}
