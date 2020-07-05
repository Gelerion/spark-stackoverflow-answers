package com.gelerion.spark.streaming.book.sinks

import com.gelerion.spark.Spark
import com.gelerion.spark.streaming.book.kafka.{Rate, SensorData}
import org.apache.spark.sql.functions._


object StreamingKafkaSinks extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //In a minimal implementation, we must ensure that our streaming DataFrame or Dataset has a value
    //field of BinaryType or StringType. The implication of this requirement is that we usually need to encode our
    //data into a transport representation before sending it to Kafka.
    //When the key is not specified, Structured Streaming will replace the key with null. This makes the Kafka sink
    //use a round-robin assignment of the partition for the corresponding topic.

    /*
    Optionally, we can control the destination topic at the record level by adding a topic field.
    If present, the topic value must correspond to a Kafka topic. Setting the topic on the writeStream option overrides
    the value in the topic field.
     */

    val kafkaBootstrapServer = "localhost:9092"
    val targetTopic = "sensors"

    val baseStream = spark.readStream
      .format("rate")
      .option("recordsPerSecond", 100)
      .load()

    val sensorData = baseStream.as[Rate]
      .map(_ => SensorData.randomGen(100000))

    //1.
    val kafkaFormattedStream = sensorData.select(
      $"id" as "key",
      to_json(
        struct($"id", $"timestamp", $"sensorType", $"value")
      ) as "value"
    )

    val kafkaWriterQuery = kafkaFormattedStream.writeStream
      .queryName("kafkaWriter")
      .outputMode("append")
      .format("kafka") // determines that the kafka sink is used
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("topic", targetTopic)
      .option("checkpointLocation", "spark-checkpoint")
      .option("failOnDataLoss", "false") // use this option when testing
//      .start()

    //2. When we add topic information at the record level, we must omit the topic configuration option.
    val kafkaFormattedStreamV2 = sensorData.select(
      $"id" as "key",
      $"sensorType" as "topic",
      to_json(struct($"id", $"timestamp", $"value")) as "value"
    )

    //Note that we have removed the setting option("topic", targetTopic) and added a topic field to each record
    val kafkaWriterQueryV2 = kafkaFormattedStreamV2.writeStream
      .queryName("kafkaWriter")
      .outputMode("append")
      .format("kafka") // determines that the kafka sink is used
      .option("kafka.bootstrap.servers", kafkaBootstrapServer)
      .option("checkpointLocation", "/path/checkpoint")
      .option("failOnDataLoss", "false") // use this option when testing
//      .start()


  }

}
