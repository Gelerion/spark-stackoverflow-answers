package com.gelerion.spark.streaming.book.sources

object StreamingFileSource {

  def main(args: Array[String]): Unit = {
//    val fileStream = spark.readStream
//      .format("parquet")
//      .option("path", "hdfs://data/exchange")
//      .schema(schema)
//      .load()

    //Common Options
    /*
    maxFilesPerTrigger (Default: unset)
      Indicates how many files will be consumed at each query trigger. This setting limits the number of files
      processed at each trigger, and in doing so, it helps to control the data inflow in the system.

    latestFirst (Default: false)
      When this flag is set to true, newer files are elected for processing first.
      Use this option when the most recent data has higher priority over older data.

     maxFileAge (Default: 7 days)
       Defines an age threshold for the files in the directory. Files older than the threshold will not be
       eligible for processing and will be effectively ignored. This threshold is relative to the most recent
       file in the directory and not to the system clock. For example, if maxFileAge is 2 days and the most
       recent file is from yesterday, the threshold to consider a file too old will be older than three days ago.
       This dynamic is similar to watermarks on event time.

     fileNameOnly (Default: false)
       When set to true, two files will be considered the same if they have the same name; otherwise, the full path will be considered.
     */

  }

}
