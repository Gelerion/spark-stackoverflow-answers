package com.gelerion.spark.streaming.book.logs

import java.sql.Timestamp

import com.gelerion.spark.Spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, Encoders}

import scala.util.matching.Regex

object ApacheLogsAnalysis extends Spark {
  import spark.implicits._

  /**
   * The logs are an ASCII file with one line per request, with the following columns:   *
   *  - Host making the request. A hostname when possible, otherwise the Internet address if the name could not be looked up.   *
   *  - Timestamp in the format “DAY MON DD HH:MM:SS YYYY,” where DAY is the day of the week, MON is the name of the month,
   *    DD is the day of the month, HH:MM:SS is the time of day using a 24-hour clock, and YYYY is the year. The timezone is –0400.
   *  - Request given in quotes.
   *  - HTTP reply code.
   *  - Bytes in the reply.
   */

  val urlExtractor: Regex = """^GET (.+) HTTP/\d.\d""".r
  val allowedExtensions: Set[String] = Set(".html", ".htm", "")

  def main(args: Array[String]): Unit = {
//    batch()

    //Spark Thrift Server can be embedded inside a Spark application, exposing DataFrames though JDBC/ODBC
    //Then we can use BeelineRunner as sql query runner
    //hive.server2.thrift.http.port, default 10001
    HiveThriftServer2.startWithContext(spark.sqlContext)
    //http://www.russellspitzer.com/2017/05/19/Spark-Sql-Thriftserver/

    val stream = spark.readStream
      .format("socket") //TextSocketSource
//      .option("host", "localhost")
      .option("host", "10.50.0.157")
      .option("port", 9999)
      .load()

    val webLogSchema = Encoders.product[WebLog].schema

    val jsonStream = stream.select(from_json($"value", webLogSchema) as "record")
    val webLogStream: Dataset[WebLog] = jsonStream.select("record.*").as[WebLog]

    println(s"is streaming: ${webLogStream.isStreaming}")

    //As we can observe, attempting to execute the count query in the following code snippet will result in an AnalysisException:
    //val count = webLogStream.count()
    //> org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;

    val contentPageLogs: String => Boolean = url => {
      val ext = url.takeRight(5).dropWhile(c => c != '.')
      allowedExtensions.contains(ext)
    }

    val urlWebLogStream = webLogStream.flatMap { weblog =>
      weblog.request match {
        case urlExtractor(url) if contentPageLogs(url) => Some(weblog.copy(request = url))
        case _ => None
      }
    }

    val rankingURLStream = urlWebLogStream
      .groupBy($"request", window($"timestamp", "5 seconds", "1 second"))
      .count()
      .orderBy(desc("count"))

    //To start a Structured Streaming job, we need to specify a sink and an output mode.
    val query = rankingURLStream.writeStream
      //with queryName, we can provide a name for the query that is used by some sinks and also presented in the job description in the Spark Console
      .queryName("urlranks") //table name
      .outputMode("complete")
      //The optional trigger option lets us specify the frequency at which we want the results to be produced.
      // - ProcessingTime(<interval>) -> lets us specify a time interval that will dictate the frequency of the query results.
      // - Once() -> A particular Trigger that lets us execute a streaming job once. It is useful for testing and also
      //             to apply a defined streaming job as a single-shot batch operation.
      // - Continuous(<checkpoint-interval>) -> This trigger switches the execution engine to the experimental
      //                                        continuous engine for low-latency processing. The checkpoint-interval
      //                                        parameter indicates the frequency of the asynchronous checkpointing for data resilience.
      .trigger(Trigger.ProcessingTime("10 seconds"))
      //Although show requires an immediate materialization of the query, and hence it’s not possible on a streaming
      //Dataset, we can use the console sink to output data to the screen.
      .format("console") //show
//      .format("memory")


    //To materialize the streaming computation, we need to start the streaming process. Finally, start() materializes
    //the complete job description into a streaming computation and initiates the internal scheduling process that
    //results in data being consumed from the source, processed, and produced to the sink.

    //start() returns a StreamingQuery object, which is a handle to manage the individual life cycle of each query.
    //This means that we can simultaneously start and stop multiple queries independently of one other within the same sparkSession.
    val streamingCtx = query.start()

    //The memory sink outputs the data to a temporary table of the same name given in the queryName option.
    spark.sql("show tables").show()

    streamingCtx.awaitTermination()
  }

  private def batch(): Unit = {
    val logsDirectory = "src/main/resources/streaming/*"
    val rawLogs = spark.read.json(logsDirectory)

    // we need to narrow the `Interger` type because the JSON representation is interpreted as `BigInteger`
    val preparedLogs = rawLogs.withColumn("http_reply", $"http_reply".cast(IntegerType))
    val weblogs = preparedLogs.as[WebLog]

    println(s"rws count: ${weblogs.count()}")

    //what was the most popular URL per day?
    val topDailyURLs = weblogs.withColumn("dayOfMonth", dayofmonth($"timestamp"))
      .select($"request", $"dayOfMonth")
      .groupBy($"dayOfMonth", $"request")
      .agg(count($"request").alias("count"))
      .orderBy(desc("count"))

    topDailyURLs.show()
    //Top hits are all images. What now? It’s not unusual to see that the top URLs are images commonly used across a site.
    //Our true interest lies in the content pages generating the most traffic. To find those, we first filter on html
    //content and then proceed to apply the top aggregation

    val contentPageLogs = weblogs.filter { log =>
      log.request match {
        case urlExtractor(url) =>
          val ext = url.takeRight(5).dropWhile(c => c != '.')
          allowedExtensions.contains(ext)
        case _ => false
      }
    }

    val topContentPages = contentPageLogs
      .withColumn("dayOfMonth", dayofmonth($"timestamp"))
      .select($"request", $"dayOfMonth")
      .groupBy($"dayOfMonth", $"request")
      .agg(count($"request").alias("count"))
      .orderBy(desc("count"))

    topContentPages.show(false)
  }
}

case class WebLog(host: String,
                  timestamp: Timestamp,
                  request: String,
                  http_reply: Int,
                  bytes: Long)

