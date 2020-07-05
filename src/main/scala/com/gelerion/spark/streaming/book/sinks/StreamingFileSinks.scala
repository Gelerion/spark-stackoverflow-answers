package com.gelerion.spark.streaming.book.sinks

import com.gelerion.spark.Spark
import org.apache.spark.sql.streaming.Trigger

object StreamingFileSinks extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    /*
    val query = stream.writeStream
      .format("csv")
      .option("sep", "\t")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .option("path","<dest/path>")
      .option("checkpointLocation", "<checkpoint/path>")
      .start()

      In this example, we are using the csv format to write the stream results to the <dest/path> destination
      directory using TAB as the custom separator. We also specify a checkpointLocation, where the checkpoint
      metadata is stored at regular intervals.

      The File sink supports only append as outputMode, and it can be safely omitted in the writeStream declaration.
     */

    val stream = spark.readStream.format("rate").load()

    //If we let this query run for a little while and then we check the target directory, we should observe a large number of small files
//    val query = stream.writeStream
//      .format("json")
//      .option("path","file-sink")
//      .option("checkpointLocation", "spark-checkpoint")
//      .start()
    /*
    # Count the files in the directory
    $ ls -1 | wc -l
    562

    # A moment later
    $ ls -1 | wc -l
    608

    # What's the content of a file?
    $ cat part-00007-e74a5f4c-5e04-47e2-86f7-c9194c5e85fa-c000.json
    {"timestamp":"2018-05-13T19:34:45.170+02:00","value":104}
     */

    val query = stream.writeStream
      .format("json")
      .option("path","file-sink")
      .trigger(Trigger.ProcessingTime("1 minute")) // <-- Add Trigger configuration
      .option("checkpointLocation", "spark-checkpoint")
      .start()

    //!! The number of records per file depends on the DataFrame partitioning
    /*
    If you are trying this example on a personal computer, the number of partitions defaults to the number of cores present.
    In our case, we have eight cores, and we observe seven or eight records per partition. That’s still very few records,
    but it shows the principle that can be extrapolated to real scenarios.

    Even though a trigger based on the number of records or the size of the data would arguably be more interesting in this
    scenario, currently, only time-based triggers are supported. This might change in the future, as Structured Streaming evolves.
     */

  }

}
