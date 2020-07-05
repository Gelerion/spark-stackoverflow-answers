package com.gelerion.spark.streaming.book.sinks

object ReliableSinks {

  def main(args: Array[String]): Unit = {
    /*
    To participate in the end-to-end reliable data delivery, sinks must provide an idempotent write operation.
    Idempotent means that the result of executing the operation two or more times is equal to executing the operation once.
    When recovering from a failure, Spark might reprocess some data that was partially processed at the time the failure
    occurred. At the side of the source, this is done by using the replay functionality.

    The combination of a replayable source and an idempotent sink is what grants Structured Streaming its
    effectively exactly once data delivery semantics.

    Reliable Sinks
    The sinks considered reliable or production ready provide well-defined data delivery semantics and are resilient
    to total failure of the streaming process.

    The following are the provided reliable sinks:

    - The File sink
      This writes data to files in a directory in the filesystem. It supports the same file formats as the
      File source: JSON, Parquet, comma-separated values (CSV), and Text.

    - The Kafka sink
      This writes data to Kafka, effectively keeping the data “on the move.” This is an interesting option
      to integrate the results of our process with other streaming frameworks that rely on Kafka as the data backbone.
     */
  }

}
