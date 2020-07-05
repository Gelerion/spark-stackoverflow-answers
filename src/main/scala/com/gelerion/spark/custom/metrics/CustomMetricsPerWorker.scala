package com.gelerion.spark.custom.metrics

import com.gelerion.spark.Spark
import com.gelerion.spark.so.general.GeneralAnswers.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ExecutorPlugin, TaskContext}
import org.apache.spark.sql.functions._
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}

object CustomMetricsPerWorker /*extends Spark*/ {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
//    spark.range(50)

    val s2 = SparkSession
      .builder()
      .appName("af-so")
      .master("local[1]")
      .config("spark.executor.plugins", CustomPlugin.getClass.getName.replace("$", ""))
      .getOrCreate()

    s2.range(0, 50).repartition().show()

    s2.stop()
  }



}

object CustomPlugin extends ExecutorPlugin {

  override def init(): Unit = {
    println(s"Started")
    //TaskContext.get() throws NPE, task context hasn't been initialized yet
  }

  override def shutdown(): Unit = {
//    println(s"Shutdown: ${TaskContext.get().partitionId()}")
    println(s"Shutdown:")
  }
}


