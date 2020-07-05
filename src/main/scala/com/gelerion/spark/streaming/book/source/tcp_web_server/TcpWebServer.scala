package com.gelerion.spark.streaming.book.source.tcp_web_server

import com.gelerion.spark.streaming.book.logs.WebLog
import org.apache.spark.sql.{Dataset, SparkSession}
import java.net._
import java.io._
import java.sql.Timestamp

import com.gelerion.spark.Spark
import com.gelerion.spark.streaming.book.logs.ApacheLogsAnalysis.spark

import scala.concurrent.Future
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.concurrent.ExecutionContext.Implicits.global

object TcpWebServer extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val logsDirectory = "src/main/resources/streaming/*"
    val rawLogs = spark.read.json(logsDirectory)

    // we need to narrow the `Interger` type because the JSON representation is interpreted as `BigInteger`
    val preparedLogs = rawLogs.withColumn("http_reply", $"http_reply".cast(IntegerType))
    val weblogs = preparedLogs.as[WebLog]


    val server = new SocketHandler(spark, 9999, weblogs)
    server.start()
  }

}

class SocketHandler(sparkSession: SparkSession, port: Int, data: Dataset[WebLog]) {
  val logDelay = 500 //millis
  @volatile var active = false

  // non blocking start of the socket handler
  def start(): Unit = {
    active = true
    new Thread() {
      override def run() {
//        connectionWidget.append("Server starting...")
        println("Server starting...")
        val server: ServerSocket = new ServerSocket(port)
        acceptConnections(server)
        server.close()
//        connectionWidget.append("Server stopped")
        println("Server stopped")
      }
    }.start()
  }

  def stop() {
    active = false
  }

  @tailrec
  final def acceptConnections(server: ServerSocket): Unit = {
//    System.out.println("Server is listening on port " + port)
    val socket = server.accept()
    println("Accepting connection from: " + socket)
    serve(socket)
    if (active) {
      acceptConnections(server)
    } else {
      () // finish recursing for new connections
    }
  }

  // 1-thread per connection model for example purposes.
  def serve(socket: Socket): Unit = {
    import sparkSession.implicits._

    val minTimestamp = data.select(min($"timestamp")).as[Timestamp].first
    val now = System.currentTimeMillis
    val offset = now - minTimestamp.getTime
    val offsetData = data.map(weblog => weblog.copy(timestamp = new Timestamp(weblog.timestamp.getTime + offset)))
    val jsonData = offsetData.toJSON
    val iter = jsonData.toLocalIterator.asScala
    new Thread() {
      override def run() {
        val out = new PrintStream(socket.getOutputStream)
//        connectionWidget.append("Starting data stream for: " + socket.getInetAddress() + "]")
        println("Starting data stream for: " + socket.getInetAddress + "]")
        while (iter.hasNext && active && !out.checkError()) {
          val data = iter.next()
          out.println(data)
//          dataWidget.append(s"[${socket.getInetAddress()}] sending: ${data.take(40)}...")
          println(s"[${socket.getInetAddress}] sending: ${data.take(40)}...")
          out.flush()
          Thread.sleep(logDelay)
        }
        out.close()
        socket.close()
      }
    }.start()
  }
}
