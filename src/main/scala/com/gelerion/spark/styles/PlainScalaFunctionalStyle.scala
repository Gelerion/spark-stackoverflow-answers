package com.gelerion.spark.styles

import java.util

import com.gelerion.spark.Spark
import org.apache.spark.sql.catalyst.expressions.ArrayContains
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, collect_set, concat_ws, hash, lit, lower, sum, when}
import org.apache.spark.sql.types.StructType
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object PlainScalaFunctionalStyle extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val cardinalityPath = args(0)
    val dt = args(1)

    val fields = Set("city", "sub1", "sub2")
    val replacements = Map("city" -> "Exceeded_City", "sub1" -> "Exceeded_Sub1", "sub2" -> "Exceeded_Sub2")

    val baseDf = spark.emptyDataFrame

    //example with currying
    val loadOneDayOfData: (String, String) => DataFrame = loadOneDay(cardinalityPath)// dt, fieldName => rawGrayList
    val loadRangeOfData: String => DataFrame = loadRange(dt, 7, loadOneDayOfData) //fieldName => combinedRawGrayList
    val createGrayList: String => DataFrame = aggregateRawGrayList(loadRangeOfData) //fieldName => grayList
    val cardinalityEnforcement: DataFrame => DataFrame = enforceCardinality(fields, replacements, createGrayList) //baseDf => afterApplyingCardinalityD

    //usage
    baseDf.transform(cardinalityEnforcement)

    // Or in one line
    /*
    val enforceCardinalityProgram =
    enforceCardinality(fields, replacements, aggregateRawGrayList(loadRange(dt, 7, loadOneDay(cardinalityPath))))

    baseDf.transform(enforceCardinalityProgram)

     //How do we handle exceptions? Should the code return Either type at each step?
    cohortETL(enforceCardinalityProgram, spark.emptyDataFrame)
    */
  }

  def cohortETL(enforceCardinalityFn: DataFrame => DataFrame,
                //a lot more functions to come...
                loadTopicDataFn: => DataFrame): DataFrame = {
    loadTopicDataFn.transform(enforceCardinalityFn)
  }

  def enforceCardinality(fields: Set[String], replacements: Map[String, String], getAggregatedGrayListFn: String => DataFrame)
                        (baseDf: DataFrame): DataFrame = {
    val collectedGrayLists: Iterable[(String, (util.List[Row], StructType))] = fields
      .map(field => (field, getAggregatedGrayListFn(field)))
      .map {
        case (attr, df) =>
          val msg = s"Collecting gray list to driver for $attr"
          baseDf.sparkSession.sparkContext.setJobDescription(msg)
          (attr, collectDataFrame(df))}

    val attributeToGrayListDF: Map[String, DataFrame] = collectedGrayLists.map{
      case (field, (data, schema)) =>
        (field, createDataFrameFromDriver(data, schema))
    }.toMap

    baseDf.sparkSession.sparkContext.setJobDescription("Applying cardinality and saving results")

    val withCardinalityKeys = fields.foldLeft(baseDf)((df, field) => df
      .transform { df =>
        field match {
          case "eventName" =>
            df.withColumn(field + "_cardinality_key", concat_ws("|", $"appId", $"ltvHourMillis"))
          case _ =>
            df.withColumn(field + "_cardinality_key", concat_ws("|", $"appId", $"ltvHourMillis", $"media_source"))

        }
      })

    val afterApplyingCardinality = fields.foldLeft(withCardinalityKeys)((df, field) => {

      val attrName = field
      df
        .join(attributeToGrayListDF(field), Seq(attrName + "_cardinality_key"), "leftouter")
        .withColumn("_exceeded_limit", $"$field".isNotNull and col(attrName + "_set").isNotNull and !new Column(ArrayContains(col(attrName + "_set").expr, hash(col(attrName)).expr)))
        .withColumn(attrName, when(col("_exceeded_limit"), replacements(field)).otherwise($"$field"))
        .drop("_exceeded_limit", attrName + "_cardinality_key", attrName + "_set")
    })

    afterApplyingCardinality
  }

  //do not trigger eager evaluation of raw gray list (aka by name parameters)
  def aggregateRawGrayList(loadGrayListForField: String => DataFrame)(fieldName: String): DataFrame  = {
    val installDateMillisColName = "install_date_millis"
    val fieldNameSetColName: String = s"${fieldName}_set"
    val groupingColumnNames = fieldName match {
      case "eventName" => Seq(installDateMillisColName, "app_id")
      case _ => Seq(installDateMillisColName, "app_id", "unmasked_media_source")
    }

    // Hash the field values to make the gray lists much smaller and easier to broadcast !!
    val valueToCollect = fieldName match {
      case "eventName" =>
        hash(lower(col(fieldName)))
      case _ =>
        hash(col(fieldName))
    }

    val grayListDf = loadGrayListForField(fieldName)
      .select((groupingColumnNames :+ fieldName).map(col): _*)
      .groupBy(groupingColumnNames.map(col): _*)
      .agg(collect_set(valueToCollect).alias(fieldNameSetColName))
      .select(concat_ws("|", groupingColumnNames.map(col): _*).alias(fieldName + "_cardinality_key"), col(fieldNameSetColName))
    grayListDf
  }

  //how do we compose functions?
  def loadRange(startDate: String, timeRange: Int, loadOneDay: (String, String) => DataFrame)(fieldName: String): DataFrame = {
    val parsedStartDate = DateTime.parse(startDate)
    val dateRange = (0 until timeRange).map(parsedStartDate.minusDays)
    val dtInMillis = System.currentTimeMillis()//Utils.getAsMillisUTC(dt)

    //par ?? - would be better to have this more explicitly
    // dateTimeToString - how to avoid implicit dependency?
    dateRange.par
      .map(date => loadOneDay(dateTimeToString(date), fieldName))
      .map(grayList => grayList.withColumn("install_date_millis", lit(dtInMillis)))
      .reduceLeft(_.union(_))
  }

  //side effect, should be handled by interpreter and not run directly
  //aka functional effects - describe procedural effects, they can be interpreted later
  //
  //three string in a row, how not to be confused?
  def loadOneDay(cardinalityPath: String)(dt: String, fieldName: String): DataFrame = {
    val grayListLocation = cardinalityPath + s"/dt=$dt/field_name=$fieldName/"
    spark.read.parquet(s"$grayListLocation")
  }

  private def dateTimeToString(dt: DateTime): String = {
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    fmt.print(dt)
  }

  private def collectDataFrame(df: DataFrame): (util.List[Row], StructType) = {
    (df.collectAsList(), df.schema)
  }

  private def createDataFrameFromDriver(data: util.List[Row], schema: StructType): DataFrame = {
    spark.createDataFrame(data, schema)
  }
}
