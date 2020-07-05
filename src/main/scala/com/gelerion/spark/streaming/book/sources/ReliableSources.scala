package com.gelerion.spark.streaming.book.sources

object ReliableSources {

  /*
  1. At t1, the system calls getOffset and obtains the current offset for the source.
  2. At t2, the system obtains the batch up to the last known offset by calling getBatch(start, end).
     Note that new data might have arrived in the meantime.
  3. At t3, the system commits the offset and the source drops the corresponding records.

  This process repeats constantly, ensuring the acquisition of streaming data. To recover from eventual
  failure, offsets are often checkpointed to external storage.

  ! Reliable Sources Must Be Replayable
  In Structured Streaming, replayability is the capacity to request a part of the stream that had been
  already requested but not committed yet.
  By requiring replayability from the source, Structured Streaming delegates recovery responsibility to the source.
  This implies that only reliable sources work with Structured Streaming to create strong end-to-end delivery guarantees.

  Available Sources

  File
   Allows the ingestion of data stored as files. In most cases, the data is transformed in records that are further
   processed in streaming mode. This supports these formats: JSON, CSV, Parquet, ORC, and plain text.

  Kafka
   Allows the consumption of streaming data from Apache Kafka.

  Socket
   A TCP socket client able to connect to a TCP server and consume a text-based data stream. The stream must be encoded in the UTF-8 character set.

  Rate
   Produces an internally generated stream of (timestamp, value) records with a configurable production rate.
   This is normally used for learning and testing purposes.

   */

}
