package com.gelerion.spark.streaming.book.windows

import com.gelerion.spark.Spark
import com.gelerion.spark.streaming.book.kafka.{Rate, WeatherMetrics}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.TimestampType


object TimeBasedWindows extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val raw = spark.readStream
      .format("rate")
      .option("recordsPerSecond", 5)
      .load().as[Rate]
      .map(_ => WeatherMetrics.randomGen)

    val timeStampEvents = raw
      .withColumn("timestamp", $"ts".cast(TimestampType))

    val perMinuteAvg = timeStampEvents
//      .withWatermark("timestamp", "1 minute")
      .withWatermark("timestamp", "0 minutes")
      .groupBy(window($"timestamp", "30 seconds"))
      .agg(avg($"pressure"))

    //Record Deduplication
    //Nevertheless, this base method is discouraged, as it requires you to store all received values for the set of
    //fields defining a unique record, which can potentially be unbounded.
    //val deduplicatedStream = stream.dropDuplicates(<field> , <field>, ...)

    perMinuteAvg.writeStream
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("complete").format("console").start()
      .awaitTermination()
  }

}
