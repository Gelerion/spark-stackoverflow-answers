package com.gelerion.spark.so.hadoop

import java.nio.charset.StandardCharsets
import java.util.{Formatter, Locale}

import com.gelerion.spark.Spark
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.sql.functions._

import scala.io.{Codec, Source}

object HadoopApiSpark extends Spark{
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    readCustomDelimiter()
  }

  //https://stackoverflow.com/questions/57952692/record-delimeter-issue-with-u00c3-while-text-processing-in-spark
  def readCustomDelimiter(): Unit = {
    val path = "src/main/resources/customDelim.txt"
    val recDelimiter = "\u00c3"

    var conf = spark.sparkContext.hadoopConfiguration
    conf.set("textinputformat.record.delimiter", recDelimiter)

    val rawRDD = spark
      .sparkContext
      .newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(_._2.toString)
    println(rawRDD.count())
  }

  def escapeUnicode(input: String): String = {
    val b = new java.lang.StringBuilder(input.length)
    val f = new Formatter(b)
    for (c <- input.toCharArray) {
      if (c < 128) b.append(c)
      else {
        val int = c.toInt
        f.format("\\u%04x", new Integer(int))
      }
    }
    b.toString
  }

}
