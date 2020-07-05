package com.gelerion.spark.so.general

import com.gelerion.spark.Spark
import com.gelerion.spark.so.general.GeneralAnswers.spark
import org.apache.spark.sql.functions._
import com.databricks.spark.xml.XmlReader

object XmlParsing extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //https://github.com/databricks/spark-xml

    parseXmlRdd()
  }

  //https://stackoverflow.com/questions/57988159/parse-xml-string-with-spark-xml-in-scala
  def parseXmlRdd(): Unit = {
    val df = Seq(
      Data("123abc", 12345,"<XML><Date Depart=\"2019-06-30\" Arrive=\"2019-06-22\" /><Passengers><Passenger Age=\"ADT\" Quantity=\"1\" /><Passenger Age=\"CHD\" Quantity=\"1\" /></Passengers><Destination Code=\"LAX\"/></XML>")).toDF

    df.show()

    val xrdd = df.select("xmldata")
      .map(a => a.getString(0)).rdd

    val xmldf = (new XmlReader())
      .xmlRdd(spark, xrdd)
      .select(
        $"Date._Arrive".as("Arrive"),
        $"Date._Depart".as("Depart"),
        $"Destination._Code".as("Destination"),
        explode($"Passengers.Passenger").alias("Passenger"))

  }

}

case class Data(id: String, code: Int, xmldata: String)