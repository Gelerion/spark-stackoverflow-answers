package com.gelerion.spark.so.batch.windows

import com.gelerion.spark.Spark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, max, rand, row_number, sum, when}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}

object CardinalityEnforcement extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    createGrayListTest()
//      createGrayListByEventName()
  }

  def createGrayListTest(): Unit = {
    val campaignsDf: DataFrame = Seq(
      ("ap1", "ms1", "c1", 10, 3),
      ("ap1", "ms1", "c2", 9, 4),
      ("ap1", "ms1", "c3", 8, 9),
      ("ap1", "ms1", "c4", 8, 5),
      // Unrelated App
      ("ap9", "ms9", "c9", 11, 1),
      // Unrelated media_source
      ("ap1", "ms2", "c1", 1, 1),
      // Some malformed data
      ("", null, "c3", 11, 1),
      ("ap1", "ms4", null, 11, 1))
      .toDF("app_id", "unmasked_media_source", "campaign", "installs_count", "clicks_count")

    val nAllowedCampaigns = 3
    val nSaltBins = 2

    val grayList = CardinalityProcessor.createGrayList(
      field = $"campaign",
      partitionBy = Seq($"app_id", $"unmasked_media_source"),
      orderBy = Seq($"installs_count".desc, $"clicks_count".desc),
      nAllowedCampaigns,
      nSaltBins)(spark)(campaignsDf)

    val campaignGreyList = grayList.select("app_id", "unmasked_media_source", "campaign")

    campaignGreyList.show(false)
    /*
    +------+---------------------+--------+
    |app_id|unmasked_media_source|campaign|
    +------+---------------------+--------+
    |ap1   |ms1                  |c1      |
    |ap1   |ms1                  |c2      |
    |ap1   |ms1                  |c3      |
    +------+---------------------+--------+
     */
  }

  def createGrayListByEventName(): Unit = {
    val DT = "2018-01-10"

    val eventsDf = Seq(
      ("ap1", "", "event-empty-date", 20),
      ("ap1", "2018-01-01", "eventName1", 1),
      ("ap1", "2018-01-01", "eventName2", 2),
      ("ap1", "2018-01-02", "eventName3", 3),
      ("ap1", "2018-01-03", "eventName4", 4),
      ("ap1", "2018-01-04", "eventName5", 5),
      ("ap1", "2018-01-05", "eventName6", 6),
      ("ap1", "2018-01-06", "eventName7", 7),
      ("ap1", "2018-01-06", "eventName8", 7),
      ("ap1", null, "event-null", 10),
      ("ap1", "2018-01-07", "eventName8", 8),
      ("ap1", "2018-01-08", null, 15),
      ("ap1", "2018-01-08", "eventName9", 9),
      ("ap1", "2018-01-08", "eventName10", 9),
      ("ap1", "2018-01-09", "eventName10", 10),
      ("ap1", "2018-01-10", "eventName10", 10),
      ("ap1", "2018-01-11", "eventName12", 12))
      .toDF("app_id", "install_date", "event_name", "event_count")

    CardinalityProcessor.createEventsGrayList(eventsDf, DT, 5)(spark)
  }
}

object CardinalityProcessor {

  def createEventsGrayList(df: DataFrame, dt: String, eventsCardinalityThreshold: Int)(spark: SparkSession): DataFrame = {
    import spark.implicits._
    //sctx.setJobGroup("main", s"creating gray list for event name")

    val windowedByAppAndEvent = df.filter($"event_name".isNotNull)
      .filter($"install_date".isNotNull && $"install_date" <= dt) // remove data from the future
      // Cardinality of install_date grouped by app_id and event_name will be low -- no skew issues
      .withColumn("row_num", row_number().over(Window.partitionBy($"app_id", $"event_name").orderBy($"install_date".desc)))

    /*
      +------+------------+----------------+-----------+-------+
      |app_id|install_date|event_name      |event_count|row_num|
      +------+------------+----------------+-----------+-------+
      |ap1   |2018-01-02  |eventName3      |3          |1      |
      |ap1   |2018-01-01  |eventName2      |2          |1      |
      |ap1   |            |event-empty-date|20         |1      |
      |ap1   |2018-01-01  |eventName1      |1          |1      |
      |ap1   |2018-01-06  |eventName7      |7          |1      |
      |ap1   |2018-01-04  |eventName5      |5          |1      |
      |ap1   |2018-01-05  |eventName6      |6          |1      |
      |ap1   |2018-01-03  |eventName4      |4          |1      |
      |ap1   |2018-01-08  |eventName9      |9          |1      |
      |ap1   |2018-01-10  |eventName10     |10         |1      |
      |ap1   |2018-01-09  |eventName10     |10         |2      |
      |ap1   |2018-01-08  |eventName10     |9          |3      |
      |ap1   |2018-01-07  |eventName8      |8          |1      |
      |ap1   |2018-01-06  |eventName8      |7          |2      |
      +------+------------+----------------+-----------+-------+
     */

    val mostRecentEvents = windowedByAppAndEvent
      .filter($"row_num" === 1) // take the most recent install_date for each event
      .drop($"row_num")

    mostRecentEvents.show(false)

    /*
      +------+------------+----------------+-----------+
      |app_id|install_date|event_name      |event_count|
      +------+------------+----------------+-----------+
      |ap1   |2018-01-02  |eventName3      |3          |
      |ap1   |2018-01-01  |eventName2      |2          |
      |ap1   |            |event-empty-date|20         |
      |ap1   |2018-01-01  |eventName1      |1          |
      |ap1   |2018-01-06  |eventName7      |7          |
      |ap1   |2018-01-04  |eventName5      |5          |
      |ap1   |2018-01-05  |eventName6      |6          |
      |ap1   |2018-01-03  |eventName4      |4          |
      |ap1   |2018-01-08  |eventName9      |9          |
      |ap1   |2018-01-10  |eventName10     |10         |
      |ap1   |2018-01-07  |eventName8      |8          |
      +------+------------+----------------+-----------+
     */

    mostRecentEvents
      .transform(createGrayList(
        $"event_name",
        Seq($"app_id"),
        // In case an app had a cardinality bug in the past that is now corrected, first allow all the events
        // from the most recent install_date
        Seq($"install_date".desc, $"event_count".desc),
        eventsCardinalityThreshold)(spark))

      /*
      df.filter($"event_name".isNotNull)
      .filter($"install_date".isNotNull && $"install_date" <= dt) // remove data from the future
      // Cardinality of install_date grouped by app_id and event_name will be low -- no skew issues
      .withColumn("row_num", row_number().over(Window.partitionBy($"app_id", $"event_name").orderBy($"install_date".desc)))
      .filter($"row_num" === 1) // take the most recent install_date for each event
      .drop($"row_num")
      // Now that we only have one row for each event_name, we can use the regular gray list implementation
      .transform(createGrayList(
        $"event_name",
        Seq($"app_id"),
        // In case an app had a cardinality bug in the past that is now corrected, first allow all the events
        // from the most recent install_date
        Seq($"install_date".desc, $"event_count".desc),
        eventsCardinalityThreshold))
        */
  }

  def createGrayList(field: Column,
                     partitionBy: Seq[Column],
                     orderBy: Seq[Column],
                     cardinalityThreshold: Int,
                     nWindowSaltBins: Int = 100)(spark: SparkSession)(df: DataFrame): DataFrame = {
    import spark.implicits._

    // First divide each window into many smaller salted windows.
    // Then remove rows from these salted windows when they contain more than cardinalityThreshold rows.
    // It is safe to remove these rows because if a salted window exceeds cardinality then
    // the unsalted window will definitely also exceed.
    // The number of rows in each salted window will be roughly the total cardinality of the window
    // divided by nWindowSaltBins.  The scalability of this step will depend on nWindowSaltBins, the
    // total cardinality of the windows and the number of spark partitions.  Some tuning may be required
    // in the future.
    val saltedWindow = Window.partitionBy(partitionBy :+ $"salt" :_*)  //partitionBy = Seq($"app_id", $"unmasked_media_source"),
    val saltedResult = df
      .withColumn("salt", Salt.salt(nWindowSaltBins))
      .withColumn("row_num", row_number().over(saltedWindow.orderBy(orderBy :_*))) //orderBy = Seq($"installs_count".desc, $"clicks_count".desc)
      // Any rows beyond cardinalityThreshold are definitely not needed.
      .filter($"row_num" <= cardinalityThreshold + 1) // need to make sure we exceed in the unsalted window

    //saltedResult.show(false)
    /*
    +------+---------------------+--------+--------------+------------+----+-------+
    |app_id|unmasked_media_source|campaign|installs_count|clicks_count|salt|row_num|
    +------+---------------------+--------+--------------+------------+----+-------+
    |ap1   |ms1                  |c1      |10            |3           |1   |1      |
    |ap1   |ms1                  |c2      |9             |4           |1   |2      |
    |ap1   |ms1                  |c4      |8             |5           |1   |3      |
    |ap1   |ms4                  |null    |11            |1           |1   |1      |
    |ap1   |ms1                  |c3      |8             |9           |0   |1      |
    |ap1   |ms2                  |c1      |1             |1           |0   |1      |
    |      |null                 |c3      |11            |1           |0   |1      |
    |ap9   |ms9                  |c9      |11            |1           |1   |1      |
    +------+---------------------+--------+--------------+------------+----+-------+
     */


    // Now window they data again without salt.
    // Importantly, now no window will have more than cardinalityThreshold * nWindowSaltBins rows
    // in it making the window calculations scalable.
    val window = Window.partitionBy(partitionBy:_*)
    val exceededCardinality = saltedResult
      .withColumn("row_num", row_number().over(window.orderBy(orderBy :_*)))
      // If the maximum row number in the window is more than the cardinality threshold, then this window
      // exceeded the cardinality threshold.  Since we removed rows from the salted windows above, we cannot
      // compute the total cardinality anymore, but we can still know if a particular window
      // exceeded the cardinality limit.
      .withColumn("max_row_num", max("row_num").over(window))
      .withColumn("exceeded_cardinality", when($"max_row_num" > cardinalityThreshold, true).otherwise(false))


    //exceededCardinality.show(false)
    /*
    +------+---------------------+--------+--------------+------------+----+-------+-----------+--------------------+
    |app_id|unmasked_media_source|campaign|installs_count|clicks_count|salt|row_num|max_row_num|exceeded_cardinality|
    +------+---------------------+--------+--------------+------------+----+-------+-----------+--------------------+
    |ap1   |ms2                  |c1      |1             |1           |0   |1      |1          |false               |
    |ap9   |ms9                  |c9      |11            |1           |1   |1      |1          |false               |
    |ap1   |ms1                  |c1      |10            |3           |1   |1      |4          |true                |
    |ap1   |ms1                  |c2      |9             |4           |1   |2      |4          |true                |
    |ap1   |ms1                  |c3      |8             |9           |0   |3      |4          |true                |
    |ap1   |ms1                  |c4      |8             |5           |1   |4      |4          |true                |
    |ap1   |ms4                  |null    |11            |1           |1   |1      |1          |false               |
    |      |null                 |c3      |11            |1           |0   |1      |1          |false               |
    +------+---------------------+--------+--------------+------------+----+-------+-----------+--------------------+
     */

    // We only store data on windows where the cardinality was exceeded.  This is why it is called a
    // gray list rather than a white list or a black list.
    exceededCardinality
      .filter($"exceeded_cardinality" === true)
      .filter($"row_num" <= cardinalityThreshold)
      .select(partitionBy :+ field :_*)

    /*
      // First divide each window into many smaller salted windows.
      // Then remove rows from these salted windows when they contain more than cardinalityThreshold rows.
      // It is safe to remove these rows because if a salted window exceeds cardinality then
      // the unsalted window will definitely also exceed.
      // The number of rows in each salted window will be roughly the total cardinality of the window
      // divided by nWindowSaltBins.  The scalability of this step will depend on nWindowSaltBins, the
      // total cardinality of the windows and the number of spark partitions.  Some tuning may be required
      // in the future.
      val saltedWindow = Window.partitionBy(partitionBy :+ $"salt" :_*)
      val saltedResult = df
        .withColumn("salt", Salt.salt(nWindowSaltBins))
        .withColumn("row_num", row_number().over(saltedWindow.orderBy(orderBy :_*)))
        // Any rows beyond cardinalityThreshold are definitely not needed.
        .filter($"row_num" <= cardinalityThreshold + 1) // need to make sure we exceed in the unsalted window

      // Now window they data again without salt.
      // Importantly, now no window will have more than cardinalityThreshold * nWindowSaltBins rows
      // in it making the window calculations scalable.
      val window = Window.partitionBy(partitionBy:_*)
      saltedResult
        .withColumn("row_num", row_number().over(window.orderBy(orderBy :_*)))
        // If the maximum row number in the window is more than the cardinality threshold, then this window
        // exceeded the cardinality threshold.  Since we removed rows from the salted windows above, we cannot
        // compute the total cardinality anymore, but we can still know if a particular window
        // exceeded the cardinality limit.
        .withColumn("max_row_num", max("row_num").over(window))
        .withColumn("exceeded_cardinality", when($"max_row_num" > cardinalityThreshold, true).otherwise(false))
        // We only store data on windows where the cardinality was exceeded.  This is why it is called a
        // gray list rather than a white list or a black list.
        .filter($"exceeded_cardinality" === true)
        .filter($"row_num" <= cardinalityThreshold)
        .select(partitionBy :+ field :_*)
     */
  }
}


object Salt {

  def salt(nSaltBins: Int) = {
    if (nSaltBins >= 1)
      (rand * nSaltBins).cast(IntegerType)
    else
      throw new IllegalArgumentException("nSaltBins must be greater than or equal to 1")
  }

  /*
   nSaltBins should probably be roughly similar to the number of partitions, though it likely depends on the data.
   Higher numbers mean more work for the second group-by stage but probably better data distribution up to
   some point when the data is distributed as well as possible.  Testing would needed to determine an optimal number,
   though preliminary tests show that values for nSaltBins within 10 fold of the number of partitions seem to work well.
    */
  def saltedGroupBy(nSaltBins: Int,
                    groupByFields: Seq[String],
                    targetMetrics: Seq[Column], // Don't use countDistinct
                    metricsAs: Seq[String])(df: DataFrame): DataFrame = {
    val agg1 = targetMetrics.zip(metricsAs).map { case (met, metAs) => met.as(metAs) }
    val agg2 = metricsAs.map(metAs => sum(col(metAs)).as(metAs))
    df
      .withColumn("salt", salt(nSaltBins))
      .groupBy("salt", groupByFields: _*)
      .agg(agg1.head, agg1.tail: _*)
      .groupBy(groupByFields.head, groupByFields.tail: _*)
      .agg(agg2.head, agg2.tail: _*)
  }

  def saltedDistinct(nSaltBins: Int)(df: DataFrame): Dataset[Row] = {
    df
      .withColumn("salt", salt(nSaltBins))
      .distinct
      .drop("salt")
      .distinct
  }
}

object CardinalityConfig {
  val fields = Map(
    "campaign" -> 3000
  )

  val defaultCardinalityPath = "s3a://.../cardinality_gray_lists"
  val exceededCardinalityReplacements = Map(
    "campaign" -> "Exceeded_Campaign_Limit"
  )
  val CARD_TIME_RANGE = 7
}
