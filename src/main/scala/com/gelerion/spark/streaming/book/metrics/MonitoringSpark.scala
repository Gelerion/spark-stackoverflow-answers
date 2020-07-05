package com.gelerion.spark.streaming.book.metrics

import java.util.concurrent.{Executors, TimeUnit}

import com.gelerion.spark.Spark
import com.gelerion.spark.streaming.book.kafka.iot.SensorDataKafkaGenerator.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}

object MonitoringSpark /*extends Spark*/ {

  /*
  Available through the Spark core engine, the Spark metrics subsystem offers a configurable metrics
  collection and reporting API with a pluggable sink interface

  Spark comes with several such sinks, including HTTP, JMX, and comma-separated values (CSV) files.
  In addition to that, there’s a Ganglia sink that needs additional compilation flags due to licensing restrictions.

  The HTTP sink is enabled by default. It’s implemented by a servlet that registers an endpoint on the driver
  host on the same port as the Spark UI. The metrics are accessible at the /metrics/json endpoint.
  Other sinks can be enabled through configuration.
   */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("monitoring-example")
      //enable metrics
      .config("spark.sql.streaming.metricsEnabled", true)
      .getOrCreate()

    //With this configuration in place, the metrics reported will contain three additional metrics
    //for each streaming query running in the same SparkSession context:

    /*
    - inputRate-total
     The total number of messages ingested per trigger interval
    - latency
     The processing time for the trigger interval
    - processingRate-total
     The speed at which the records are being processed
     */

    import spark.implicits._

    val stream = spark.readStream.format("rate").option("recordsPerSecond", 10).load()
    val query = stream.writeStream
      .format("memory")
      .queryName("test")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    val background = Executors.newSingleThreadScheduledExecutor()
    background.scheduleAtFixedRate(() => {
      //shows a brief snapshot of what the query is currently doing.
      println(query.status)

      //retrieves any fatal exception encountered by the execution of the query
      println(s"exception? ${query.exception}")

      //
    }, 1, 1, TimeUnit.SECONDS )

    //block the current thread untie the query ends
    query.awaitTermination()

    //stops the execution of the query
    query.stop()

    background.shutdownNow()

    /*
    From the perspective of monitoring the job’s performance, we are particularly interested
    in numInputRows, inputRowsPerSecond, and processedRowsPerSecond. These self-describing fields provide key
    indicators about the job performance. If we have more data than our query can process, inputRowsPerSecond
    will be higher than processedRowsPerSecond for sustained periods of time. This might indicate that the cluster
    resources allocated for this job should be increased to reach a sustainable long-term performance.
     */

    // ------------- StreamingQueryListener
    // Gangila
    // sparkSession.streams.addListener(chartListener)
    val chartListener = new StreamingQueryListener {
      val maxDataPoints = 100
      // a mutable reference to an immutable container to buffer n data points
      //var data: List[Metric] = Nil

      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
//        val queryProgress = event.progress
//        // ignore zero-values events
//        if (queryProgress.numInputRows > 0) {
//          val time = queryProgress.timestamp
//          val input = Metric("in", time, event.progress.inputRowsPerSecond)
//          val processed = Metric("proc", time, event.progress.processedRowsPerSecond)
//          data = (input :: processed :: data).take(maxDataPoints)
//          chart.applyOn(data)
//        }
      }
    }
  }

}
