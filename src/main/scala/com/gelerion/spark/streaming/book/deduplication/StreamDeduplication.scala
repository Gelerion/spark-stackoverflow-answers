package com.gelerion.spark.streaming.book.deduplication

object StreamDeduplication {

  def main(args: Array[String]): Unit = {
    /*
    STREAM DEDUPLICATION
    We discussed that distinct on an arbitrary stream is computationally difficult to implement.
    But if we can define a key that informs us when an element in the stream has already been seen,
    we can use it to remove duplicates:

    stream.dropDuplicates("<key-column>") …
     */
  }

}
