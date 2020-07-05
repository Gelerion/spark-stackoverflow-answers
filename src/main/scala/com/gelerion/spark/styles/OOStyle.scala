package com.gelerion.spark.styles

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, concat_ws, when}
import org.joda.time.DateTime

object OOStyle {

  // read layer
  trait GrayListRepository {
    def load(date: DateTime, field: Field): DataFrame
  }

  //aggregate logic
  trait GrayListAggregator {
    def aggregate(schema: GrayListSchema, rawGrayList: DataFrame): GrayList
  }

  //model
  case class Field(name: String, replacement: String)
  case class GrayListSchema(field: Field) {
    val installDateColName: String = "install_date_millis"
    val groupByCols: Seq[Column] = Seq(col(installDateColName)) /*+: field.partitionedBy.map(toFieldCol)*/
    val cardinalityKeyColName: String = field.name + "_cardinality_key"
    val cardinalityKeyCol: Column = concat_ws("|", groupByCols:_*).alias(cardinalityKeyColName)
    val setColName: String =  s"${field.name}_set"
  }
  case class GrayList(schema: GrayListSchema, data: DataFrame)

  // BL layer
  class CardinalityEnforcementService(val repository: GrayListRepository, val aggregator: GrayListAggregator) {

    def enforce(date: DateTime, fields: Seq[Field])(baseDf: DataFrame): DataFrame = {
      val originalColumns: Array[String] = baseDf.columns

      fields
        // load is loosely coupled
        .map(field => (field, repository.load(date, field)))
        // aggregation is loosely coupled
        .map{case (field, rawGrayList) => aggregator.aggregate(GrayListSchema(field), rawGrayList)}
        // now the logic itself
        .foldLeft(baseDf)((df, grayList) => {
          val field = grayList.schema.field
          df
            .join(grayList.data, Seq(grayList.schema.cardinalityKeyColName), "left_outer")
            .withColumn("_exceeded_limit", col("exceededLimitColumn")) //some logic here
            .withColumn(field.name, when(col("_exceeded_limit"), field.replacement).otherwise(col(field.name)))
            .select(originalColumns.head, originalColumns.tail:_*)
        })
    }
  }

  //Now we have control over date ranges, computed fields, composable services and full support of unit test

  // ------------------------------

  // Usage
  trait Services {
    val cardinality: CardinalityEnforcementService
    //...
  }

  // injecting implementations
  class Cohort(services: Services) {
    def runETL(): Unit = {
      //spark.read.parquet...
      // .transform(logic)
      // .transform(services.cardinality.enforce(...))
    }
  }
}
