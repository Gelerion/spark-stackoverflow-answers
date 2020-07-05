package com.gelerion.spark.so.batch.windows

import com.gelerion.spark.Spark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

//https://habr.com/ru/post/268983/
object ConsecutiveColumnStatus extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    consecutiveColumnStatus()
  }

  //https://stackoverflow.com/questions/57188727/how-to-find-which-date-the-consecutive-column-status-complete-started-with-in
  def consecutiveColumnStatus(): Unit = {
    /*
    Requirement: 1. go Back 8 days (this is easy) 2. So we are on 20190111 from below data frame, I need to check
    day by day from 20190111 to 20190104 (7 day period) and get a date on which status has 'complete'
    for consecutive 7 days. So we should get 20190108
     */
    val events = spark.createDataFrame(Seq(
      EventStatus(1, "20190101", "complete"),
      EventStatus(2, "20190102", "complete"),
      EventStatus(3, "20190103", "complete"),
      EventStatus(4, "20190104", "complete"),
      EventStatus(5, "20190105", "complete"),
      EventStatus(6, "20190106", "complete"),
      EventStatus(7, "20190107", "complete"),
      EventStatus(8, "20190108", "complete"),
      EventStatus(9, "20190109", "pending"),
      EventStatus(10, "20190110", "complete"),
      EventStatus(11, "20190111", "complete"),
      EventStatus(12, "20190112", "pending"),
      EventStatus(13, "20190113", "complete"),
      EventStatus(14, "20190114", "complete"),
      EventStatus(15, "20190115", "pending"),
      EventStatus(16, "20190116", "pending"),
      EventStatus(17, "20190117", "pending"),
      EventStatus(18, "20190118", "pending"),
      EventStatus(19, "20190119", "pending"),
      //-----------------------------------
      EventStatus(20, "20190120", "complete"),
      EventStatus(21, "20190121", "complete"),
      EventStatus(22, "20190122", "complete"),
      EventStatus(23, "20190123", "complete"),
      EventStatus(24, "20190124", "complete"),
      EventStatus(25, "20190125", "complete"),
      EventStatus(26, "20190126", "complete"),
      EventStatus(27, "20190127", "complete"),
      EventStatus(28, "20190128", "complete")
//      EventStatus(29, "20190129", "complete")
    )).select($"id", to_date($"date", "yyyyMMdd").as("date"), $"status")

    /*
    Expected output:
    +---+--------+--------+
    | id|    date|  status|
    +---+--------+--------+
    |  1|20190101|complete|
    |  2|20190102|complete|
    |  3|20190103|complete|
    |  4|20190104|complete|
    |  5|20190105|complete|
    |  6|20190106|complete|
    |  7|20190107|complete|
    |  8|20190108|complete|
     */

//    events.show(false)
    // ====== Solution
    val id_window = Window.orderBy("id")
    //lag_status - previous column status
    val windowed = events.select($"*", lag($"status", 1).over(id_window).as("lag_status"))
      //peek first
      //complete|null -> complete
      //complete|pending -> complete
      .withColumn("lag_stat", coalesce($"lag_status", $"status")).drop($"lag_status")

    //1  |2019-01-01|complete|complete|1
    //9  |2019-01-09|pending |complete|0
    val withStatusFlag = windowed.select($"*", ($"status" === $"lag_stat").cast(IntegerType).as("status_flag"))

    //Window by status flag
    val status_flag_window = Window.orderBy($"id".desc).rangeBetween(Window.currentRow , Window.currentRow + 7)
    val withPrevValues = withStatusFlag.select($"*", sum($"status_flag").over(status_flag_window).as("previous_7_sum"))

    //generate 7 dates back
    val exploded = withPrevValues.where($"previous_7_sum" === 8)
      .select($"date")
      .select(explode(sequence(date_sub($"date", 7), $"date")).as("date"))

    val result = withPrevValues.join(exploded, Seq("date"), "inner")
        .select($"id", $"date", $"status")
//      .select($"id", concat_ws("", split($"date".cast("string"), "-")).as("date"), $"status")

    withPrevValues.show(30, false)

    result.explain(true)
    events.show(30,false)
    result.show(30,false)

  }
}

case class EventStatus(id: Int, date: String, status: String)