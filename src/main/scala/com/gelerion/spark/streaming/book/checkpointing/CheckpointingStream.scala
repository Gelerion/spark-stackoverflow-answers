package com.gelerion.spark.streaming.book.checkpointing

object CheckpointingStream {

  def main(args: Array[String]): Unit = {

    /*
    Setting the duration on the DStream is optional. If not set, it defaults to a specific value,
    depending on the DStream type. For MapWithStateDStream, it defaults to 10x the duration of the batch interval.
    For all other DStreams, it defaults to 10 seconds or the batch interval, whichever is largest.

    The checkpoint frequency must be a multiple of the batch interval, or the job will fail with an initialization error.
    Intuitively, what we want is the interval frequency to be each n batch intervals, where the choice of n depends
    on the volume of the data and how critical the requirements are for failure recovery of the job. A rule of thumb,
    advised in the Spark documentation, is to do checkpointing every five to seven batch intervals as a start.
     */

    /*
    Checkpoint Tuning
    Thanks to the Spark user interface, you can measure on average how much more time you will need for a batch interval
    computation that includes checkpointing compared to the batch processing time that you observe in RDDs that do
    not require checkpointing. Suppose that our batch processing time is about 30 seconds for a batch interval that is
    one minute. This is a relatively favorable case: at every batch interval, we spend only 30 seconds computing,
    and we have 30 seconds of “free” time during which our processing system is sitting idle while receiving data.

    Given our application requirements, we decide to checkpoint every five minutes. Now that we’ve decided to do
    that checkpointing every five minutes, we run some measurements and observe that our checkpointing batches require
    four minutes of actual batch processing time. We can conclude that we will need about three and a half minutes
    to write to disk, considering that during that same batch we have also expanded 30 seconds in computation.
    It means that in this case, we will require four batches to checkpoint once again. Why is that?

    This is because when we spend three and a half minutes actually writing our files to disk, we are in fact still
    receiving data when those three and a half minutes elapse. In those three and a half minutes, we have received
    three and a half new batches that we did not have time to process because our system was blocked waiting for
    the checkpoint operation to end. As a consequence, we now have three and a half—that is, four batches of data
    that are stored on our system—that we need to process to catch up and reach stability again. Now, we have a
    computation time on a normal batch of 30 seconds, which means that we will be able to catch up by one new batch
    on every batch interval of one minute, so that in four batches we will be caught up with the received data.
    We will be able to checkpoint again at the fifth batch interval. At a checkpointing interval of five batches,
    we are in fact just at the stability limit of the system.
     */

    /*
    NOTE
    There exists another alternative to this discipline of checkpoint interval tuning, which consists of writing
    to a persistent directory that accepts data at a very high speed. For that purpose, you can choose to point
    your checkpoint directory to a path of an HDFS cluster that is backed by very fast hardware storage,
    such as solid-state drives (SSDs). Another alternative is to have this backed by memory with a decent
    offload to disk when memory is filled, something performed by Alluxio, for example—a project that was initially
    developed as Tachyon, some module of Apache Spark. This simple method for reducing the checkpointing time and
    the time lost in checkpointing is often one of the most efficient ways to reach a stable computation under
    Spark Streaming; that is, if it is affordable, of course.
     */
  }

}
