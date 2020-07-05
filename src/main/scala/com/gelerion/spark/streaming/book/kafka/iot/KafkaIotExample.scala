package com.gelerion.spark.streaming.book.kafka.iot

import com.gelerion.spark.Spark
import com.gelerion.spark.streaming.book.kafka.SensorData

import scala.util.Try

/*
We create a small but complete Internet of Things (IoT)-inspired streaming program.
Our use case will be to consume a stream of sensor readings from Apache Kafka as the streaming source.

We are going to correlate incoming IoT sensor data with a static reference file that contains all known sensors
with their configuration. That way, we enrich each incoming record with specific sensor parameters that we require
to process the reported data. We then save all correctly processed records to a file in Parquet format.
 */
object KafkaIotExample extends Spark  {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    /*
    The Kafka schema

    root
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true)
     |-- partition: integer (nullable = true)
     |-- offset: long (nullable = true)
     |-- timestamp: timestamp (nullable = true)
     |-- timestampType: integer (nullable = true)

     In general, Structured Streaming requires the explicit declaration of a schema for the consumed stream.
     In the specific case of kafka, the schema for the resulting Dataset is fixed and is independent of the contents of the stream.
     */

    val kafkaBootstrapServer = "localhost:9092"
    val topic = "iot-data"
    val workDir = ""
    val referenceFile = "sensor-records.parquet"
    val targetPath = "enriched-sensors-data"

    val rawData = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("subscribe", topic) //specifies the topic or topics to subscribe to
      .option("startingOffsets", "earliest") //The offset reset policy to apply when this application starts out fresh
      .load()

    val iotData = rawData.select($"value").as[String].flatMap{record =>
      val fields = record.split(",")
      Try {
        SensorData(fields(0).toInt, fields(1).toLong, fields(2).toDouble)
      }.toOption
    }

    val sensorRef = spark.read.parquet(s"$referenceFile")
    sensorRef.cache()

    val sensorWithInfo = sensorRef
      .join(iotData, Seq("sensorId"), "inner")

    val knownSensors = sensorWithInfo
      //compute the real values of the sensor reading using the minimum-maximum ranges in the reference data
      .withColumn("dnvalue", $"value" * ($"maxRange" - $"minRange") + $"minRange")
      .drop("value", "maxRange", "minRange")

//    knownSensors.explain(true)

    //The final step of our streaming application is to write the enriched IoT data to a Parquet-formatted file
//    val knownSensorsQuery = knownSensors.writeStream
//      .outputMode("append")
//      //specify the sink that will materialize the result downstream. In our case, we use the built-in FileStreamSink with Parquet format.
//      .format("parquet")
//      .option("path", targetPath)
//      .option("checkpointLocation", "/tmp/checkpoint")
//      .start()

    val knownSensorsQuery = knownSensors.writeStream
      .outputMode("append")
      .format("console")
      .start()

//    val loggingThread = new Thread(() => {
//      while (true) {
//        Thread.sleep(10000)
//        knownSensorsQuery.recentProgress.foreach(println)
//        println("-------------------------------------------------------")
//      }
//    })
//    loggingThread.setDaemon(true)
//    loggingThread.start()

    knownSensorsQuery.awaitTermination()

  }

}
