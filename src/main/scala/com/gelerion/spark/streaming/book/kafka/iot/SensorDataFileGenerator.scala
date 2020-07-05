package com.gelerion.spark.streaming.book.kafka.iot

import com.gelerion.spark.Spark
import com.gelerion.spark.streaming.book.kafka.{SensorReference, SensorType}

import scala.util.Random

object SensorDataFileGenerator extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val sensorCount = 100000
//    val workDir = "/tmp/streaming-with-spark/"
    val workDir = ""
    val referenceFile = "sensor-records.parquet"

    val sensorTypes = List(
      SensorType("humidity", "%Rh", 0, 100),
      SensorType("temperature", "oC", -100, 100),
      SensorType("brightness", "lux", 0, 100000),
      SensorType("rainfall", "mm/day", 0, 5000),
      SensorType("windspeed", "m/s", 0, 50),
      SensorType("pressure", "mmHg", 800, 1100),
      SensorType("magnetism", "T", 0, 1000),
      SensorType("Radiation", "mSv", 0.01, 10000))

    val sensorIds = spark.range(0, sensorCount)

    val sensors = sensorIds.map { id =>
      val sensorType = sensorTypes(Random.nextInt(sensorTypes.size))
      SensorReference(id, sensorType.sensorType, sensorType.unit, sensorType.minRange, sensorType.maxRange)
    }

    sensors.show()

    sensors
      .write
      .mode("overwrite")
      .parquet(s"$referenceFile")
  }

}
