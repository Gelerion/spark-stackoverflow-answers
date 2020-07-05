package com.gelerion.spark.streaming.book.kafka

import scala.util.Random

package object iot

case class SensorType(sensorType: String, unit: String, minRange: Double, maxRange: Double)
case class SensorReference(sensorId: Long, sensorType: String, unit: String, minRange: Double, maxRange: Double)

case class Rate(timestamp: Long, value: Long)

case class SensorData(sensorId: Int, timestamp: Long, value: Double) {
  def asKafkaRecord: String = s"$sensorId,$timestamp,$value"
}

object SensorData {
  def randomGen(maxId:Int): SensorData = {
    SensorData(Random.nextInt(maxId), System.currentTimeMillis, Random.nextDouble())
  }
}

case class WeatherMetrics(id: String, ts:Long, pressure: Double, temperature: Double)
object WeatherMetrics {
  def randomGen: WeatherMetrics = {
    WeatherMetrics(Random.nextInt(10000).toString, System.currentTimeMillis / 1000, Random.nextDouble() * 100, Random.nextDouble() * 100)
  }
}