package com.gelerion.spark.streaming.book.kafka.iot

import com.gelerion.spark.Spark
import com.gelerion.spark.streaming.book.kafka.{Rate, SensorData}

object SensorDataKafkaGenerator extends Spark {
  import spark.implicits._

  /**
   * Install kafka
   * curl https://www-us.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz -o kafka_2.12-2.3.0.tgz
   * tar xzf kafka_2.12-2.3.0.tgz
   * ln -sf kafka_2.12-2.2.0 kafka
   *
   * Launch ZK & Kafka
   * kafka/bin/zookeeper-server-start.sh -daemon kafka/config/zookeeper.properties
   * kafka/bin/kafka-server-start.sh -daemon kafka/config/server.properties
   *
   * Create topics
   * kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic iot-data
   */

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServer = "localhost:9092"
    val targetTopic = "iot-data"
    val workDir = "/tmp/streaming-with-spark"
    val sensorCount = 100000

    //We use the built-in rate generator as the base stream for our data generator
    val baseStream = spark.readStream
      .format("rate")
      .option("recordsPerSecond", 100)
      .load()

    val sensorValues = baseStream.as[Rate]
      .map(_ => SensorData.randomGen(sensorCount))
      .map(_.asKafkaRecord)


    val query = sensorValues
      .writeStream
      .format("kafka")
      .queryName("kafkaWriter")
      .outputMode("append")
      .option("kafka.bootstrap.servers", kafkaBootstrapServer) // comma-separated list of host:port
      .option("topic", targetTopic)
      .option("checkpointLocation", workDir + "/generator-checkpoint")
      .option("failOnDataLoss", "false") // use this option when testing
      .start()

    query.awaitTermination()
  }

}
