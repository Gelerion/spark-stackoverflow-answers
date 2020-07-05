package com.gelerion.spark.custom.enrichments

import com.gelerion.spark.Spark
import com.gelerion.spark.so.general.GeneralAnswers.spark
import org.apache.spark.sql.{FastString, SparkSession}
import org.apache.spark.{ExecutorPlugin, TaskContext}
import org.apache.spark.sql.functions._
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

object CustomSparkUDTEnricher /*extends Spark*/ {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val s2 = SparkSession
      .builder()
      .appName("af-so")
      .master("local[1]")
      .getOrCreate()

    Seq("foo").toDF.as[FastString].show()
  }



}



