package com.gelerion.spark.streaming.timing

import com.gelerion.spark.Spark
import com.gelerion.spark.so.general.GeneralAnswers.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, TimestampType}

object SplitIntoTimeWindow extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    serverInfo()
  }

  def serverInfo(): Unit = {
//    spark.readStream.option("header", "true")
//      .csv("/Users/denisshuvalov/Learning/Spark/spark-stack-overflow/src/main/resources/servers_agent_data.csv")
//      .groupBy(window($"time_stamp", "5 minutes"), $"Hostname")
//      .agg()

    val in = spark.read.option("header", "true")
      .csv("/Users/denisshuvalov/Learning/Spark/spark-stack-overflow/src/main/resources/servers_agent_data.csv")
      .withColumn("timestamp", unix_timestamp($"time_stamp", "yyyy/MM/dd HH:mm:ss").cast(TimestampType))
      .drop($"time_stamp")

    in.printSchema()
//    in.show(false)

    val total = col("metrics")("Total")
    val used = col("metrics")("used")
    val buffer = col("metrics")("buffer")
    val cached = col("metrics")("cached")

    val joinMap = udf { values: Seq[Map[String, Double]] => values.flatten.toMap }
    val grouped =
      in
      .groupBy(window($"timestamp", "5 minutes"), $"Hostname")
      .agg(
        joinMap(collect_list(map($"kpi_subtype", $"value_current".cast(DoubleType)))).as("metrics")
      )
//      .printSchema()
        .select($"window", $"Hostname",
          (total - ((total - used + buffer + cached) / total) * 100).as("percentage")
        )

    grouped.explain(true)
    grouped.printSchema()
    grouped.show(false)
  }
}
