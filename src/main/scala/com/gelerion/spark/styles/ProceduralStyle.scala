package com.gelerion.spark.styles

import com.gelerion.spark.Spark
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.util

import org.apache.spark.sql.catalyst.expressions.ArrayContains

object ProceduralStyle {

  class EnforceCardinalityProgram extends Spark {
    import spark.implicits._

    //1. Many hardcoded cardinality values
    //2. Read function is hardcoded and can't be substituted
    //3. Aggregation and lookup functions are defined in two different places though are closely related
    //4. How can we test this code? mock data, mock aggregation logic, mock gray list creation?
    //5. Single responsibility?
    def enforceCardinality(dt: String, cardinalityPath: String)(baseDf: DataFrame): DataFrame = {
      val cardinalityAttributes = Set("city", "sub1", "sub2")
      val exceededCardinalityReplacements = Map("city" -> "Exceeded_City", "sub1" -> "Exceeded_Sub1", "sub2" -> "Exceeded_Sub2")

      val collectedGrayLists: Iterable[(String, (util.List[Row], StructType))] = cardinalityAttributes
        .map(attr => (attr, loadRangeOfCardinalityListsAsDf(7, attr, dt, cardinalityPath)))
        .map {
          case (attr, df) =>
            val msg = s"Collecting gray list to driver for $attr"
            baseDf.sparkSession.sparkContext.setJobDescription(msg)
            (attr, collectDataFrame(df))}

      val attributeToGrayListDF: Map[String, DataFrame] = collectedGrayLists.map{
        case (attr, (data, schema)) =>
          (attr, createDataFrameFromDriver(data, schema))
      }.toMap

      baseDf.sparkSession.sparkContext.setJobDescription("Applying cardinality and saving results")

      val withCardinalityKeys = cardinalityAttributes.foldLeft(baseDf)((df, attr) => df
        .transform { df =>
          attr match {
            case "eventName" =>
              df.withColumn(attr + "_cardinality_key", concat_ws("|", $"appId", $"ltvHourMillis"))
            case _ =>
              df.withColumn(attr + "_cardinality_key", concat_ws("|", $"appId", $"ltvHourMillis", $"media_source"))

          }
        })

      val afterApplyingCardinality = cardinalityAttributes.foldLeft(withCardinalityKeys)((df, attr) => {

        val attrName = attr
        df
          .join(attributeToGrayListDF(attr), Seq(attrName + "_cardinality_key"), "leftouter")
          .withColumn("_exceeded_limit", $"$attr".isNotNull and col(attrName + "_set").isNotNull and !new Column(ArrayContains(col(attrName + "_set").expr, hash(col(attrName)).expr)))
          .withColumn(attrName, when(col("_exceeded_limit"), exceededCardinalityReplacements(attr)).otherwise($"$attr"))
          .drop("_exceeded_limit", attrName + "_cardinality_key", attrName + "_set")
      })

      afterApplyingCardinality
    }

    def collectDataFrame(df: DataFrame): (util.List[Row], StructType) = {
      (df.collectAsList(), df.schema)
    }

    def createDataFrameFromDriver(data: util.List[Row], schema: StructType) = {
      spark.createDataFrame(data, schema)
    }

    private def loadCardinalityGrayListAsDataFrame(attribute: String, dt: String, cardinalityPath: String): DataFrame = {
      val fieldName = attribute
      val grayListLocation = cardinalityPath + s"/dt=$dt/field_name=$fieldName/"
      val installDateMillisColName = "install_date_millis"
      val fieldNameSetColName: String = s"${fieldName}_set"
      val dtInMillis = System.currentTimeMillis()//Utils.getAsMillisUTC(dt)
      val cardinalityListDf = spark.read.parquet(s"$grayListLocation").withColumn(installDateMillisColName, lit(dtInMillis))
      val groupingColumnNames = attribute match {
        case "eventName" => Seq(installDateMillisColName, "app_id")
        case _ => Seq(installDateMillisColName, "app_id", "unmasked_media_source")
      }

      // Hash the field values to make the gray lists much smaller and easier to broadcast !!
      val valueToCollect = attribute match {
        case "eventName" =>
          hash(lower(col(fieldName)))
        case _ =>
          hash(col(fieldName))
      }

      val grayListDf = cardinalityListDf
        .select((groupingColumnNames :+ fieldName).map(col): _*)
        .groupBy(groupingColumnNames.map(col): _*)
        .agg(collect_set(valueToCollect).alias(fieldNameSetColName))
        .select(concat_ws("|", groupingColumnNames.map(col): _*).alias(fieldName + "_cardinality_key"), col(fieldNameSetColName))
      grayListDf
    }

    private def loadRangeOfCardinalityListsAsDf(cardinalityTimeRange: Int, attribute: String, dt: String, cardinalityPath: String): DataFrame = {
      val startDate = DateTime.parse(dt)
      val dateRange = (0 until cardinalityTimeRange).map(startDate.minusDays)
      dateRange.par.map(date => loadCardinalityGrayListAsDataFrame(attribute, dateTimeToString(date), cardinalityPath)).reduceLeft(_.union(_))
    }

    private def dateTimeToString(dt: DateTime): String = {
      val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
      fmt.print(dt)
    }
  }

}
