package com.gelerion.spark.streaming.book.sources

object StreamingKafkaSource {
  def main(args: Array[String]): Unit = {
    /*
    val dataStream = kafkaStream.selectExpr("CAST(key AS STRING)",
                                         "CAST(value AS STRING)")
                             .as[(String, String)]


     Selecting a Topic Subscription Method

     subscribe
      Takes a single topic or a list of comma-separated topics: topic1, topic2, ..., topicn. This method subscribes
      to each topic and creates a single, unified stream with the data of the union of all the topics; for example,
      .option("subscribe", "topic1,topic3").

     subscribePattern
      This is similar to subscribe in behavior, but the topics are specified with a regular expression pattern.
      For example, if we have topics 'factory1Sensors', 'factory2Sensors', 'street1Sensors', 'street2Sensors',
      we can subscribe to all “factory” sensors with the expression .option("subscribePattern", "factory[\\d]+Sensors").

     assign
      Allows the fine-grained specification of specific partitions per topic to consume. This is known as
      TopicPartitions in the Kafka API. The partitions per topic are indicated using a JSON object, in which each
      key is a topic and its value is an array of partitions. For example, the option definition
      .option("assign", """{"sensors":[0,1,3]}""") would subscribe to the partitions 0, 1, and 3 of topic sensors.
      To use this method we need information about the topic partitioning. We can obtain the partition information
      programmatically by using the Kafka API or through configuration.

      KAFKA SOURCE-SPECIFIC OPTIONS
      The following options configure the behavior of the Kafka source. They relate in particular to how offsets are consumed:

      startingOffsets (default: latest)
       Accepted values are earliest, latest, or a JSON object representing an association between topics,
       their partitions, and a given offset. Actual offset values are always positive numbers.
       There are two special offset values: -2 denotes earliest and -1 means latest; for example, """ {"topic1": { "0": -1, "1": -2, "2":1024 }} """

      startingOffsets are only used the first time a query is started. All subsequent restarts will use the checkpoint
      information stored. To restart a streaming job from a specific offset, we need to remove the contents of the checkpoint.

      failOnDataLoss (default: true)
       This flag indicates whether to fail the restart of a streaming query in case data might be lost.
       This is usually when offsets are out of range, topics are deleted, or topics are rebalanced.
       We recommend setting this option to false during the develop/test cycle because stop/restart of the query
       side with a continuous producer will often trigger a failure. Set this back to true for production deployment.

      kafkaConsumer.pollTimeoutMs (default: 512)
       The poll timeout (in milliseconds) to wait for data from Kafka in the distributed consumers running
       on the Spark executors.

      fetchOffset.numRetries (default: 3)
       The number of retries before failing the fetch of Kafka offsets.

      fetchOffset.retryIntervalMs (default: 10)
       The delay between offset fetch retries in milliseconds.

      maxOffsetsPerTrigger (default: not set)
       This option allows us to set a rate limit to the number of total records to be consumed at each query trigger.
       The limit configured will be equally distributed among the set of partitions of the subscribed topics.
     */

    //==========================================

    /*
    Kafka Consumer Options
    val tlsKafkaSource = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1, host2:port2")
      .option("subscribe", "topsecret")
      .option("kafka.security.protocol", "SSL")
      .option("kafka.ssl.truststore.location", "/path/to/truststore.jks")
      .option("kafka.ssl.truststore.password", "truststore-password")
      .option("kafka.ssl.keystore.location", "/path/to/keystore.jks")
      .option("kafka.ssl.keystore.password", "keystore-password")
      .option("kafka.ssl.key.password", "password")
      .load()
     */
  }

}
