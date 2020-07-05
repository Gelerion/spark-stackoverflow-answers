package com.gelerion.spark.styles.mixed

import java.util

import com.gelerion.spark.styles.{AggregateStrategy, Field, LoadFieldSpec, LoadRangeSpec, Strategies}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object cardinality {

  //looks pretty hard to follow imho
  def applyCardinality(fields: Seq[Field], loadSpec: LoadRangeSpec, strategy: AggregateStrategy)
                      (loadGrayListsFn: Seq[LoadFieldSpec] => DataFrame): DataFrame => DataFrame = baseDf => {
    val fieldToGrayList = createGrayLists(fields, loadSpec, strategy)(loadGrayListsFn)
    enforceCardinality(fieldToGrayList, strategy)(baseDf)
  }

  def enforceCardinality(fieldToGrayList: Map[Field, DataFrame], strategy: AggregateStrategy)(baseDf: DataFrame): DataFrame = {
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
        .withColumn("_exceeded_limit", col("${field.name}").isNotNull and col(attrName + "_set").isNotNull and !strategy.lookup(field.name)) //Also not very understandable, imho
        .withColumn(attrName, when(col("_exceeded_limit"), field.replacement).otherwise(col("${field.name}")))
        .drop("_exceeded_limit", attrName + "_cardinality_key", attrName + "_set")
    }

    afterApplyingCardinality
  }

  def createGrayLists(fields: Seq[Field], loadSpec: LoadRangeSpec, strategy: AggregateStrategy)(loadGrayListsFn: Seq[LoadFieldSpec] => DataFrame): Map[Field, DataFrame] = {
    getLoadFieldSpecs(fields, loadSpec)
      .map { case (field, loadFieldSpecs) =>
        val grayList = loadGrayListsFn(loadFieldSpecs)
          .transform(aggregateRawGrayList(field, strategy))

        val (rows, schema) = collectDataFrame(grayList)
        (field, createDataFrameFromDriver(rows, schema)(grayList.sparkSession))
      }
  }

  def getLoadFieldSpecs(fields: Seq[Field], loadSpec: LoadRangeSpec): Map[Field, Seq[LoadFieldSpec]] = {
    fields
      .map(field =>
        field -> loadSpec.dates.map(loadDate => LoadFieldSpec(field, loadDate, loadSpec.basePath)))
      .toMap
  }

  def aggregateRawGrayList(field: Field, aggStrategy: AggregateStrategy)(rawGrayList: DataFrame): DataFrame = {
    val groupBy = (field.partitionBy :+ "install_date").toArray // install_date was added in one of the previous steps

    val grayListDf = rawGrayList
      .select((groupBy :+ field.name).map(col): _*)
      .groupBy(groupBy.map(col): _*)
      .agg(aggStrategy.aggregate(field.name))
      .select(concat_ws("|", groupBy.map(col): _*).alias(field.name + "_cardinality_key"), col(s"${field.name}_set")) //we use _set suffix in several places, also not very safe for refactoring

    grayListDf
  }

  def collectDataFrame(df: DataFrame): (util.List[Row], StructType) = {
    (df.collectAsList(), df.schema)
  }

  def createDataFrameFromDriver(data: util.List[Row], schema: StructType)(spark: SparkSession): DataFrame = {
    spark.createDataFrame(data, schema)
  }
}
