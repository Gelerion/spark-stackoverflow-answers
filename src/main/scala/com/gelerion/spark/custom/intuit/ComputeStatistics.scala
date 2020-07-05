package com.gelerion.spark.custom.intuit

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Period}

import com.gelerion.spark.Spark
import org.apache.spark.sql.functions._

object ComputeStatistics extends Spark {
  import spark.implicits._

  /*
  As our Staff Big Data Engineer, you are requested to compute a daily usage statistics
  computation of the users of our customers. You need to build a system that will do a daily batch
  processing, that will allow data analysts to query these usage statistics, and draw some insights.

  The input data is clickstream information that is kept in some file system like S3 or HDFS, in
  files that contain a bunch of JSON documents in them. Each document represents 1 event.
  Each such file represents 1 minute of data. The bucket object structure is as follows:
   */
  def main(args: Array[String]): Unit = {
    //The statistics we want to compute for each customer (client_id) are:
/*  What is the number of activities used
      - per user
      - per account
    in the last 1, 3, 7, 14, 30, 90, 180, 365 days

    What is the number of modules used
       - per user
       - per account in the last 1, 3, 7, 14, 30, 90, 180, 365 days

    What is the number of unique users per account in the last 1, 3, 7, 14, 30, 90, 180, 365 days
      */

    //select activities, modules
    // from users_statistics
    // from accounts_statistics
    // where period == 7 and client_id == 1234

    //users-accounts
    //many to many

    /*
    client_id
        user_level_stats
           - period=1
           - period=3
           - period=7
             ...
        account_level_stats
           - period=1
           - period=3
           - period=7
             ...
     */

    // 2019-12-23 = processing date
    // 2019-12-23 = 1 day period
    // 2019-12-20 = 3 days period
    // 2018-12-23 = 365 days period

    // Delta - ?

    // 2019-12-24 = processing date
    // 2018-12-24 = 365 days period

    //historical_date - delete when < (processing_date - 365)
    //each processing date calculate for every period back to 365
    // as in druid per day cal delta: (period 1)
    // atomically add/remove delta

    //to mitigate slow count distinct we could use materialized views
    //druid/clickhouse/pinot

    //days_between(processing_date, event_date) = period
    //mimic layout
    val clickstream = Seq(
      ("2019-12-23", 1234, "john", 4567, "activity_1", "module_1"),
      ("2019-12-23", 1234, "john", 9876, "activity_1", "module_1"),
      ("2019-12-23", 1234, "doe", 4567, "activity_1", "module_1"),
      //------------------------------------------------------
      ("2019-12-24", 1234, "doe", 4567, "activity_1", "module_1"),
      ("2019-12-24", 1234, "john", 9876, "activity_1", "module_1"),
      ("2019-12-24", 1234, "mike", 9876, "activity_1", "module_1"),
      //------------------------------------------------------
      ("2019-12-25", 1234, "mike", 4567, "activity_1", "module_1"),
      ("2019-12-25", 1234, "john", 9876, "activity_1", "module_1"),
      ("2019-12-25", 1234, "dan", 1111, "activity_1", "module_1"),
      //------------------------------------------------------
      ("2019-12-26", 1234, "denis", 4568, "activity_1", "module_1"),
      ("2019-12-26", 1234, "yuri", 9877, "activity_1", "module_1"),
      ("2019-12-26", 1234, "pasha", 4568, "activity_1", "module_1"),
    ).toDF("date", "client_id", "user_id", "account_id", "activity", "module")

    clickstream.createOrReplaceTempView("clickstream")
//    val result = spark.sql(
//      """
//        |select user_id, count(distinct activity)
//        |from clickstream
//        |group by user_id
//        |""".stripMargin)

    //if we use on-demand instances and we have enough capacity
    //clickstream.cache()

    // -- Daily usage sliding window approach
    // activities/modules used per user
    val userStatsDaily = clickstream
      .groupBy($"date", $"user_id".as("id"))
        .agg(count("activity").as("activities"), count("module").as("modules"))
        .withColumn("level", lit("user"))

    // activities/modules used per account
    val accountStatsDaily = clickstream
      .groupBy($"date", $"account_id".as("id"))
      .agg(count("activity").as("activities"), count("module").as("modules")) //collect_set("$user_id")
      .withColumn("level", lit("account"))

    val unifiedDailyStats = userStatsDaily.union(accountStatsDaily)

    //bucketBy(date, level)
    unifiedDailyStats.orderBy($"date", $"level").show(false)

    // 2nd phase - the most straightforward implementation
    //val periods = (1, 3, 7, 14, 30, 90, 180, 365)
/*    val processingDate = LocalDate.parse("2019-12-25")
    val pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val periods = Array(1, 3)/*.map(Period.ofDays)*/
    for (period <- periods) {
      println(s"Calculating $period")
      //load dates interval -> utility object Dates
      val dates = Stream.range(0, period).map(days => processingDate.minusDays(days)).map(date => date.format(pattern)).toList
      println(s"Dates: $dates")
      //val dailyStats = spark.load(dates)

      //Fault tolerance - check already computed periods
      unifiedDailyStats
        .where($"date".isin(dates:_*))
        .groupBy($"level", $"id")
        .agg(count("activities"), count("modules"))
        .orderBy($"level") //bucketBy
        .show(false)
    }*/

    // 2nd phase - sliding window
    // state - earliest_day_stats + current_day_stats

    //mimic previously created state for 3 days
    val state =  unifiedDailyStats
      .where($"date".isin(Array("2019-12-25", "2019-12-24", "2019-12-23"):_*))
      .groupBy($"level", $"id")
      .agg(count("activities").as("activities"), count("modules").as("modules"))

    println("Previous state")
    state.show(false)

    //mimic earliest date
    val earliestUsers = clickstream
      .where($"date" === "2019-12-23")
      .groupBy($"user_id".as("id"))
      .agg(count("activity").as("activities"), count("module").as("modules"))
      .withColumn("level", lit("user"))
    val earliestAccs = clickstream
      .where($"date" === "2019-12-23")
      .groupBy($"account_id".as("id"))
      .agg(count("activity").as("activities"), count("module").as("modules")) //collect_set("$user_id")
      .withColumn("level", lit("account"))

    val earliestDateView = earliestUsers.union(earliestAccs)
    println("Evict delta")
    earliestDateView.show(false)

    val processingDateV2 = LocalDate.parse("2019-12-26")
    val periodsV2 = Array(3)
    for (period <- periodsV2) {
      println(s"Calculating $period")
      val dateToEvict = processingDateV2.minusDays(period)
      println("Evicting from satate = " + dateToEvict)

      //load state for period
      //state = spark.load(state).where(period = 3)
      //evictDelta = spark.load(dateToEvict)

      //broadcast hash join if possible / bucket by
      val joined = state.join(earliestDateView
        .withColumnRenamed("activities", "activities_delta")
        .withColumnRenamed("modules", "modules_delta"),
        Seq("id", "level"), "left")
        .na.fill(0) //replace nulls with zero

      println("Joined data")
      joined.show(false)

      println("After delta subtracted")
      joined
        .withColumn("act_new", $"activities" - $"activities_delta")
        .withColumn("mod_new", $"modules" - $"modules_delta")
        .drop("activities", "modules", "activities_delta", "modules_delta")
        .withColumnRenamed("act_new", "activities")
        .withColumnRenamed("mod_new", "modules")
        .show(false)

      //val subDelta = loadDelta(evictDate)
      //val addDelta = loadDelta(processDate)
      //state
      // .transform(subtract(subDelta))
      // .transform(add(addDelta))
      // .write().bucketBy()

      /* With delta lake
      val evictDelta = ...

      DeltaTable.forPath(spark, "/stats/period=3/")
        .as("historical")
        .merge(
          evictDelta.as("delta"),
          "historical.id = delta.id")
        .whenMatched
        .updateExpr(
          Map("activities" -> "historical.activities - delta.activities"),
          Map("modules" -> "historical.modules - delta.modules"))
        .whenNotMatched.ignore()
        .execute()
       */


      //Hybrid solution - doesn't make sense for period fewer than 14


      //load dates interval -> utility object Dates
      //val dates = Stream.range(0, period).map(days => processingDate.minusDays(days)).map(date => date.format(pattern)).toList
//      println(s"Dates: $dates")
      //val dailyStats = spark.load(dates)

      //Fault tolerance - check already computed periods
//      unifiedDailyStats
//        .where($"date".isin(dates:_*))
//        .groupBy($"level", $"id")
//        .agg(count("activities"), count("modules"))
//        .orderBy($"level") //bucketBy
//        .show(false)
    }

    //for count(activities/modules) several assumptions have to made
    // enforcements/requirements ?



//    clickstream
//      .rollup($"user_id", $"account_id")
//      .agg(count("activity"), count("module"))
//      .show(false)

    //rollup(user_id, account_id)
    //count(activity, module)

    //spark.load(365 days of data)
    //  .withColumn(period, days_between(processing_date, event_date))
    //  .where(period is in [1,3,7,14,30,90])

    //4 parts - more robust in case of failure it won't recompute everything (bottom-up)
    // period = 1 where period = 1
    // period = 3 where  (agg_data_per_1 + raw period 2 + 3)
    // period = 7 where  (agg_data_per_3 + raw period 4 + 5 + 6)
    // period = 14 where (agg_data_per_7 + raw period 7-13)
    // period = 30 where (agg_data_per_14 + raw period 14-29)


//    result.show(false)

    //data catalog.load()
  }


}
