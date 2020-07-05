package com.gelerion.spark.arrays

import com.gelerion.spark.Spark
import com.gelerion.spark.so.batch.windows.ConsecutiveColumnStatus.spark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WorkingWithGroupingArrays extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    rollingSumCustomSize()
  }

  //https://stackoverflow.com/questions/57551807/how-to-calculate-rolling-sum-with-varying-window-sizes-in-pyspark
  //I have a spark dataframe that contains sales prediction data for some products in some stores over a time period.
  //How do I calculate the rolling sum of Predictions for a window size of next N values?
  def rollingSumCustomSize(): Unit = {
    val df = Seq(
      (1, 100, "2019-07-01",  0.92, 2),
      (1, 100, "2019-07-02",  0.62, 2),
      (1, 100, "2019-07-03",  0.89, 2),
      (1, 100, "2019-07-04",  0.57, 2),
      (2, 200, "2019-07-01",  1.39, 3),
      (2, 200, "2019-07-02",  1.22, 3),
      (2, 200, "2019-07-03",  1.33, 3),
      (2, 200, "2019-07-04",  1.61, 3)
    )
      .toDF("ProductId", "StoreId", "Date", "Prediction", "N")

    /*
    Expected Output Data
    +-----------+---------+------------+------------+---+------------------------+
    | ProductId | StoreId |    Date    | Prediction | N |       RollingSum       |
    +-----------+---------+------------+------------+---+------------------------+
    |         1 |     100 | 2019-07-01 | 0.92       | 2 | sum(0.92, 0.62)        |
    |         1 |     100 | 2019-07-02 | 0.62       | 2 | sum(0.62, 0.89)        |
    |         1 |     100 | 2019-07-03 | 0.89       | 2 | sum(0.89, 0.57)        |
    |         1 |     100 | 2019-07-04 | 0.57       | 2 | sum(0.57)              |
    |         2 |     200 | 2019-07-01 | 1.39       | 3 | sum(1.39, 1.22, 1.33)  |
    |         2 |     200 | 2019-07-02 | 1.22       | 3 | sum(1.22, 1.33, 1.61 ) |
    |         2 |     200 | 2019-07-03 | 1.33       | 3 | sum(1.33, 1.61)        |
    |         2 |     200 | 2019-07-04 | 1.61       | 3 | sum(1.61)              |
    +-----------+---------+------------+------------+---+------------------------+
     */

    val window = Window.partitionBy("ProductId", "StoreId")
      .orderBy("Date")
      .rowsBetween(Window.currentRow, Window.unboundedFollowing)

    val summed = df
      .withColumn("summed", collect_list("Prediction").over(window))

    /*
      +---------+-------+----------+----------+---+------------------------+
      |ProductId|StoreId|Date      |Prediction|N  |summed                  |
      +---------+-------+----------+----------+---+------------------------+
      |1        |100    |2019-07-01|0.92      |2  |[0.92, 0.62, 0.89, 0.57]|
      |1        |100    |2019-07-02|0.62      |2  |[0.62, 0.89, 0.57]      |
      |1        |100    |2019-07-03|0.89      |2  |[0.89, 0.57]            |
      |1        |100    |2019-07-04|0.57      |2  |[0.57]                  |
      |2        |200    |2019-07-01|1.39      |3  |[1.39, 1.22, 1.33, 1.61]|
      |2        |200    |2019-07-02|1.22      |3  |[1.22, 1.33, 1.61]      |
      |2        |200    |2019-07-03|1.33      |3  |[1.33, 1.61]            |
      |2        |200    |2019-07-04|1.61      |3  |[1.61]                  |
      +---------+-------+----------+----------+---+------------------------+
     */
//    summed.explain(true)
//    summed.show(false)

    val result = summed
    // .withColumn("summed", expr("slice(summed, 1, N)"))
    /*
    +---------+-------+----------+----------+---+------------------+
    |ProductId|StoreId|Date      |Prediction|N  |summed            |
    +---------+-------+----------+----------+---+------------------+
    |1        |100    |2019-07-01|0.92      |2  |[0.92, 0.62]      |
    |1        |100    |2019-07-02|0.62      |2  |[0.62, 0.89]      |
    |1        |100    |2019-07-03|0.89      |2  |[0.89, 0.57]      |
    |1        |100    |2019-07-04|0.57      |2  |[0.57]            |
    |2        |200    |2019-07-01|1.39      |3  |[1.39, 1.22, 1.33]|
    |2        |200    |2019-07-02|1.22      |3  |[1.22, 1.33, 1.61]|
    |2        |200    |2019-07-03|1.33      |3  |[1.33, 1.61]      |
    |2        |200    |2019-07-04|1.61      |3  |[1.61]            |
    +---------+-------+----------+----------+---+------------------+
     */


      //High Order Function (LambdaFunction)
      //https://www.waitingforcode.com/apache-spark-sql/apache-spark-2.4.0-features-array-higher-order-functions/read
      .withColumn("summed", expr("aggregate(slice(summed, 1, N), cast(0 as double), (acc, sum) -> acc + sum)"))

    result.explain(true)
    result.show(false)
  }

}

//case class OrderSummary(produ)