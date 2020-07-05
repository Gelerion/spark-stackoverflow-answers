package com.gelerion.spark.so.pivot

import com.gelerion.spark.Spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object RenamePivot extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
//    combinePivotAndAggregatedColumn()

    renamePivotColumn()
  }

  //https://stackoverflow.com/questions/37836877/rename-pivoted-and-aggregated-column-in-pyspark-dataframe
  def renamePivotColumn(): Unit = {
    val data = Array(
      (0, "A", 223, "201603", "PORT"),
      (0, "A", 22, "201602", "PORT"),
      (0, "A", 422, "201601", "DOCK"),
      (1, "B", 3213, "201602", "DOCK"),
      (1, "B", 3213, "201601", "PORT"),
      (2, "C", 2321, "201601", "DOCK"))

    val df = spark.createDataFrame(data)
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "type")
      .withColumnRenamed("_3", "cost")
      .withColumnRenamed("_4", "date")
      .withColumnRenamed("_5", "ship")

    val pivoted = df.groupBy($"id", $"type")
      .pivot("date")
      .agg(
        avg($"cost").as("avg(cost)"),
        first($"ship").as("first(ship)")
      )

    //201601_(avg(cost),mode=Complete,isDistinct=false) AS cost#1619
    //->
    //201601_avg(cost)
    pivoted.show(false)
  }

  //Combine pivoted and aggregated column in PySpark Dataframe
  //https://stackoverflow.com/questions/57191369/combine-pivoted-and-aggregated-column-in-pyspark-dataframe
  def combinePivotAndAggregatedColumn(): Unit = {
    val recipesUsage = spark.createDataFrame(Seq(
      RecipeUsage("2019-01-01", "A", 0.03, 53),
      RecipeUsage("2019-01-01", "A", 0.02, 55),
      RecipeUsage("2019-01-01", "B", 0.05, 60),
      RecipeUsage("2019-01-02", "A", 0.11, 75),
      RecipeUsage("2019-01-02", "B", 0.06, 65),
      RecipeUsage("2019-01-02", "B", 0.06, 66)
    ))

//    recipesUsage.groupBy($"date")
//    .pivot($"recipe")
//    .agg(avg($"percent").alias("percent"), avg($"volume").alias("volume"))
//    .show(false)

    /*
     date      | A_percent | A_volume | B_percent | B_volume
    --------------------------------------------------------
    2019-01-01 |   0.025   |  54      |  0.05     |  60
    2019-01-02 |   0.11    |  75      |  0.07     |  65
     */

    //However, if I aggregate just one variable, say percent, the column names don't include the aggregated variable:
    val pivoted = recipesUsage.groupBy("date")
      .pivot("recipe")
      .agg(avg("percent"))

//    pivoted.explain(true)
    pivoted.show(false)

    /*
     date      |   A   |  B
    -------------------------
    2019-01-01 | 0.025 | 0.05
    2019-01-02 | 0.11  | 0.07
     */

    rename(Set("date"), "_percent")(pivoted).show()
  }

  def rename(identity: Set[String], suffix: String)(df: DataFrame): DataFrame = {
    val fieldNames = df.schema.fields.map(filed => filed.name)
    val renamed = fieldNames.map(fieldName => {
      if (identity.contains(fieldName)) {
        fieldName
      } else {
        fieldName + suffix
      }
    } )

    df.toDF(renamed:_*)
  }


}

case class RecipeUsage(date: String, recipe: String, percent: Double, volume: Int)