package com.gelerion.spark.so.conditions

import com.gelerion.spark.Spark
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object FirstRowCond extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    firstRowBasedOnCondition()
 }

  //Getting first row based on condition
  //https://stackoverflow.com/questions/57195746/getting-first-row-based-on-condition
  def firstRowBasedOnCondition(): Unit = {
    val network = spark.createDataFrame(Seq(
      NetworkState("YYY", 20, 1, 10),
      NetworkState("YYY", 30, 0, 9),
      NetworkState("YYY", 40, 0, 8),
      NetworkState("YYY", 80, 1, 7),
      NetworkState("TTT", 50, 0, 10),
      NetworkState("TTT", 40, 1, 8),
      NetworkState("TTT", 10, 0, 4),
      NetworkState("TTT", 10, 1, 2)
    ))

    /*
    The result should look like this:

    network   volume  indicator  Hour
    YYY       20      1          10
    YYY       30      0          9
    TTT       50      0          10
    TTT       40      1          8

     */

//    network.orderBy($"network", $"hour".desc)
//      .show(false)

    //splitting your data set into two parts with indicator 1 and 0
    val indicator1 = network.filter("indicator == 1")
    val indicator0 = network.filter("indicator == 0")
    indicator0.createOrReplaceTempView("indicator0")

//    network.filter($"indicator" === 0)

    val networkWindow = Window.partitionBy($"network").orderBy($"hour".desc)
    val first0ind = indicator0
      .select($"network", $"volume", $"indicator", $"hour", row_number().over(networkWindow).as("rnk"))
      .where($"rnk" === 1)
      .select($"network", $"volume", $"indicator", $"hour")

    val subQuery = spark.sql(
      """
        |SELECT tmp.network, tmp.volume, tmp.indicator, tmp.hour,
        |       ROW_NUMBER() over (PARTITION BY tmp.network ORDER BY tmp.hour) as rnk
        |FROM indicator0 as tmp
        |""".stripMargin)

    /*
    +-------+------+---------+----+---+
    |network|volume|indicator|hour|rnk|
    +-------+------+---------+----+---+
    |YYY    |40    |0        |8   |1  |
    |YYY    |30    |0        |9   |2  |
    |TTT    |10    |0        |4   |1  |
    |TTT    |50    |0        |10  |2  |
    +-------+------+---------+----+---+
    */

//    subQuery.explain(true)
//    subQuery.show(false)

    val first0Indicator = spark.sql(
      """
        |SELECT network, volume, indicator, hour
        |FROM (
        |   SELECT tmp.network, tmp.volume, tmp.indicator, tmp.hour,
        |          ROW_NUMBER() over (PARTITION BY tmp.network ORDER BY tmp.hour DESC) as rnk
        |   FROM indicator0 as tmp
        |) i
        |WHERE rnk == 1
        |""".stripMargin)

    //merging both the dataframes back to for your required output result
    val result = indicator1.union(first0ind)
      .orderBy($"network".desc,$"Hour".desc)

    result.explain(true)
    result.show(false)

  }
}

case class NetworkState(network: String, volume: Int, indicator: Int, hour: Int)

case class Person(first: String, last: String, age: Integer)
