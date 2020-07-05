package com.gelerion.spark.styles

import java.util

import com.gelerion.spark.Spark
import com.gelerion.spark.styles.PlainScalaFunctionalStyleV2.{loadData, spark, withInstallDateColumn}
import org.apache.spark.sql.catalyst.expressions.ArrayContains
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object PlainScalaFunctionalStyleV2 extends Spark {
  import spark.implicits._

  /*
  Requirements:
   1. Modular
   2. Testable
   3. Easy to read
   4. Easy to perform changes
   */

  def main(args: Array[String]): Unit = {
    val cardinalityPath = args(0)
    val dt = ProcessDate(args(1))

    val fields: Seq[Field] = Seq(Field("city", "Exceeded_City", partitionBy = Seq("app_id")), Field("sub1", "Exceeded_Sub1", partitionBy = Seq("app_id")), Field("sub2", "Exceeded_Sub2", partitionBy = Seq("app_id")))

    val hashingStrategy = Strategies.hashingStrategy

    //repetitive field passing
    //non modular
    //code duplication and a lot of context coupling
    //extensive use of case classes
    //how to write tests?

//    val fieldToGrayList = getLoadFieldSpecs(fields, LoadRangeSpec(cardinalityPath, dt, timeRange = 7))
//      .map(loadRawGrayLists) //<- for test purposes should be "injected" outside
//      .map(aggregateRawGrayList(hashingStrategy))
//      .map(collectDataFrame)
//      .map(createDataFrameFromDriver)

    // Cohort BL
    //How do we inject enforceCardinality logic to Cohort ETL?
    val baseDf = spark.emptyDataFrame
//    baseDf.transform(enforceCardinality(fieldToGrayList, hashingStrategy))
  }

  def enforceCardinality(fieldToGrayList: Map[Field, DataFrame], aggregateStrategy: AggregateStrategy)(baseDf: DataFrame): DataFrame = {
    baseDf.sparkSession.sparkContext.setJobDescription("Applying cardinality and saving results")

    val withCardinalityKeys = fieldToGrayList.keys.foldLeft(baseDf)((df, field) => df
      .transform { df =>
        //Code duplication (:
        field.name match {
          case "eventName" =>
            df.withColumn(field + "_cardinality_key", concat_ws("|", col("appId"), col("ltvHourMillis")))
          case _ =>
            df.withColumn(field + "_cardinality_key", concat_ws("|", col("appId"), col("ltvHourMillis"), col("media_source")))

        }
      })

    val afterApplyingCardinality = fieldToGrayList.foldLeft(withCardinalityKeys) { case (df, (field, grayList)) =>
      val attrName = field.name
      df
        .join(grayList, Seq(attrName + "_cardinality_key"), "leftouter")
        .withColumn("_exceeded_limit", $"${field.name}".isNotNull and col(attrName + "_set").isNotNull and !aggregateStrategy.lookup(field.name)) //Also not very understandable, imho
        .withColumn(attrName, when(col("_exceeded_limit"), field.replacement).otherwise($"${field.name}"))
        .drop("_exceeded_limit", attrName + "_cardinality_key", attrName + "_set")
    }

    afterApplyingCardinality
  }

//  def aggregateRawGrayList(aggStrategy: AggregateStrategy)(in: (Field, DataFrame)): (Field, DataFrame) = in match {
//    case (field, rawGrayList) => aggregateRawGrayList(field, aggStrategy, rawGrayList)
//  }
//
//  def aggregateRawGrayList(field: Field, aggStrategy: AggregateStrategy, rawGrayList: DataFrame): (Field, DataFrame) = {
//    val groupBy = (field.partitionBy :+ "install_date").toArray // install_date was added in one of the previous steps
//
//    val grayListDf = rawGrayList
//      .select((groupBy :+ field.name).map(col): _*)
//      .groupBy(groupBy.map(col): _*)
//      .agg(aggStrategy.aggregate(field.name))
//      .select(concat_ws("|", groupBy.map(col): _*).alias(field.name + "_cardinality_key"), col(s"${field.name}_set")) //we use _set suffix in several places, also not very safe for refactoring
//
//    (field, grayListDf)
//  }

  //good candidate for implicit conversion?
  def loadRawGrayLists(in: (Field, Seq[LoadFieldSpec])): (Field, DataFrame) = in match {
    case (field, specs) => loadRawGrayLists(field, specs)
  }

  def loadRawGrayLists(field: Field, specs: Seq[LoadFieldSpec]): (Field, DataFrame) = {
    val rawGrayList = specs
      .map(loadData) //<- how to decouple this function?
      .map(withInstallDateColumn) //<- how to decouple this function?
      .map(_.data)
      .reduce(_.union(_))

    (field, rawGrayList)
  }

  def withInstallDateColumn(fieldData: FieldData): FieldData = {
    val df = fieldData.data
    //constant value, df schema?
    fieldData.copy(data = df.withColumn("install_date", lit(fieldData.loadDate.asMillis())))
  }

//  def getLoadFieldSpecs(fields: Seq[Field], loadSpec: LoadRangeSpec): Map[Field, Seq[LoadFieldSpec]] = {
//    fields
//      .map(field =>
//        field -> loadSpec.dates.map(loadDate => LoadFieldSpec(field, loadDate, loadSpec.basePath)))
//      .toMap
//  }

  def loadData(spec: LoadFieldSpec): FieldData = {
    FieldData(spec.field, spec.loadDate, spark.read.parquet(spec.fullPath))
  }

  def collectDataFrame(in: (Field, DataFrame)): (Field, (util.List[Row], StructType)) = in match {
    case (field, grayList) => (field, collectDataFrame(grayList))
  }

  private def collectDataFrame(df: DataFrame): (util.List[Row], StructType) = {
    (df.collectAsList(), df.schema)
  }

  def createDataFrameFromDriver(in: (Field, (util.List[Row], StructType))): (Field, DataFrame) = in match {
    case (field, (collectedGrayList, schema)) => (field, createDataFrameFromDriver(collectedGrayList, schema))
  }

  private def createDataFrameFromDriver(data: util.List[Row], schema: StructType): DataFrame = {
    spark.createDataFrame(data, schema)
  }
}

// data model
case class LoadFieldSpec(field: Field, loadDate: LoadDate, basePath: String) {
  def fullPath: String = basePath + s"/${loadDate.dt}/${field.name})"
}

case class LoadRangeSpec(basePath: String, startDate: ProcessDate, timeRange: Int = 0) {
  def dates: Seq[LoadDate] = startDate.minusDays(timeRange)
}

case class ProcessDate(dt: String, pattern: String = "yyyy-MM-dd") {
  private val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)

  def minusDays(timeRange : Int): Seq[LoadDate] = {
    (0 until timeRange).map(DateTime.parse(dt, formatter).minusDays).map(date => LoadDate(date.toString(formatter)))
  }
}

case class LoadDate(dt: String, pattern: String = "yyyy-MM-dd") {
  private val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)
  def asMillis(): Long = DateTime.parse(dt, formatter).withZone(UTC).getMillis
}

case class Field(name: String, replacement: String, partitionBy: Seq[String])
case class FieldData(field: Field, loadDate: LoadDate, data: DataFrame)

case class AggregateStrategy(aggregate: String => Column, lookup: String => Column)

object Strategies {
  val hashingStrategy: AggregateStrategy = AggregateStrategy(
    fieldName => collect_set(hash(col(fieldName))).as(s"${fieldName}_set"),
    fieldName => new Column(ArrayContains(col(s"${fieldName}_set").expr, hash(col(fieldName)).expr))
  )
}

trait GreyListRepository {
  def loadRawGrayLists(in: (Field, Seq[LoadFieldSpec])): (Field, DataFrame) = in match {
    case (field, specs) => loadRawGrayLists(field, specs)
  }

  def loadRawGrayLists(field: Field, specs: Seq[LoadFieldSpec]): (Field, DataFrame) = {
    val rawGrayList = specs
      .map(loadData) //<- how to decouple this function?
      .map(withInstallDateColumn) //<- how to decouple this function?
      .map(_.data)
      .reduce(_.union(_))

    (field, rawGrayList)
  }
}

// ------------ 1st try to modularize project
/*object Main {
  def main(args: Array[String]): Unit = {
    object cohort extends CohortETL with CardinalityModuleLive with ExtractModuleLive with TransformModuleLive with LoadModuleLive
    cohort.runETL(args)
  }
}

trait CohortETL {
  val cardinalityModule: CardinalityModule
  val extractModule: ExtractModule
  val transformModule: TransformModule
  val loadModule: LoadModule

  def runETL(cli: CLI): Unit = {
    extractModule.load(topic?)
      .transform(cardinalityModule.enforceCardinality(...))
      .transform(transformModule.renameFields)
      .transform(transformModule.alignData)
      .transform(transformModule.groupBy)
    //etc...
  }

}*/
/*
trait CardinalityModule {
  val grayListsRepo: Seq[LoadFieldSpec] => DataFrame
  val aggStrategy: AggregateStrategy

  def enforceCardinality(fields: Seq[Field], loadSpec: LoadRangeSpec)(baseDf: DataFrame): DataFrame = {
    val fieldToGrayList = getLoadFieldSpecs(fields, loadSpec)
      .map(loadRawGrayLists)
      .map(aggregateRawGrayList(aggStrategy))
      .map(collectDataFrame)
      .map(createDataFrameFromDriver)

    baseDf.transform(enforceCardinality(fieldToGrayList))
  }

  private def enforceCardinality(fieldToGrayList: Map[Field, DataFrame])(baseDf: DataFrame): DataFrame = {
    baseDf.sparkSession.sparkContext.setJobDescription("Applying cardinality and saving results")

    val withCardinalityKeys = fieldToGrayList.keys.foldLeft(baseDf)((df, field) => df
      .transform { df =>
        //Code duplication (:
        field.name match {
          case "eventName" =>
            df.withColumn(field + "_cardinality_key", concat_ws("|", col("appId"), col("ltvHourMillis")))
          case _ =>
            df.withColumn(field + "_cardinality_key", concat_ws("|", col("appId"), col("ltvHourMillis"), col("media_source")))

        }
      })

    val afterApplyingCardinality = fieldToGrayList.foldLeft(withCardinalityKeys) { case (df, (field, grayList)) =>
      val attrName = field.name
      df
        .join(grayList, Seq(attrName + "_cardinality_key"), "leftouter")
        .withColumn("_exceeded_limit", col("${field.name}").isNotNull and col(attrName + "_set").isNotNull and !aggStrategy.lookup(field.name)) //Also not very understandable, imho
        .withColumn(attrName, when(col("_exceeded_limit"), field.replacement).otherwise(col("${field.name}")))
        .drop("_exceeded_limit", attrName + "_cardinality_key", attrName + "_set")
    }

    afterApplyingCardinality
  }

  def loadRawGrayLists(in: (Field, Seq[LoadFieldSpec])): (Field, DataFrame) = in match {
    case (field, specs) => (field -> grayListsRepo(specs))
  }

  private def aggregateRawGrayList(aggStrategy: AggregateStrategy)(in: (Field, DataFrame)): (Field, DataFrame) = in match {
    case (field, rawGrayList) => aggregateRawGrayList(field, aggStrategy, rawGrayList)
  }

  private def aggregateRawGrayList(field: Field, aggStrategy: AggregateStrategy, rawGrayList: DataFrame): (Field, DataFrame) = {
    val groupBy = (field.partitionBy :+ "install_date").toArray // install_date was added in one of the previous steps

    val grayListDf = rawGrayList
      .select((groupBy :+ field.name).map(col): _*)
      .groupBy(groupBy.map(col): _*)
      .agg(aggStrategy.aggregate(field.name))
      .select(concat_ws("|", groupBy.map(col): _*).alias(field.name + "_cardinality_key"), col(s"${field.name}_set")) //we use _set suffix in several places, also not very safe for refactoring

    (field, grayListDf)
  }

  def getLoadFieldSpecs(fields: Seq[Field], loadSpec: LoadRangeSpec): Map[Field, Seq[LoadFieldSpec]] = {
    fields
      .map(field =>
        field -> loadSpec.dates.map(loadDate => LoadFieldSpec(field, loadDate, loadSpec.basePath)))
      .toMap
  }

  private def collectDataFrame(in: (Field, DataFrame)): (Field, (util.List[Row], StructType)) = in match {
    case (field, grayList) => (field, collectDataFrame(grayList))
  }

  private def collectDataFrame(df: DataFrame): (util.List[Row], StructType) = {
    (df.collectAsList(), df.schema)
  }

  private def createDataFrameFromDriver(in: (Field, (util.List[Row], StructType))): (Field, DataFrame) = in match {
    case (field, (collectedGrayList, schema)) => (field, createDataFrameFromDriver(collectedGrayList, schema))
  }

  private def createDataFrameFromDriver(data: util.List[Row], schema: StructType): DataFrame = {
    spark.createDataFrame(data, schema)
  }
}

object CardinalityModuleLive extends CardinalityModule {
  override val grayListsRepo = specs => {
    //spark.load.parquet
    spark.emptyDataFrame
  }
  override val aggStrategy = Strategies.hashingStrategy
}
*/
