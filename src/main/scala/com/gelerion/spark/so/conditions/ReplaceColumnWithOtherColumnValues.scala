package com.gelerion.spark.so.conditions

import com.gelerion.spark.Spark
import com.gelerion.spark.so.conditions.FilterConditionAsString.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ReplaceColumnWithOtherColumnValues extends Spark {

  //https://stackoverflow.com/questions/57359585/how-to-replace-string-values-in-one-column-with-actual-column-values-from-other
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    //How to replace string values in one column with actual column values from other columns in the same dataframe?
    val data = Seq(
      ("tiffany", 10, "virginia", "ChildName + ChildAge + ChildState"),
      ("andrew", 11, "california", "ChildState + ChildName + ChildAge"),
      ("tyler", 12, "ohio", "ChildAge + ChildState + ChildName")
    ).toDF("name", "age", "state", "description")

    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("state", StringType),
      StructField("description", StringType)
    ))
    val encoder = RowEncoder(schema)

    val res = data.map(row => {
      val desc = row.getAs[String]("description").replaceAll("\\s+", "").split("\\+")
      val sb = new StringBuilder()
      val map = desc.zipWithIndex.toMap.map(_.swap)

      map(0) match {
        case "ChildState" => sb.append(row.getAs[String]("state")).append(" ")
        case "ChildAge" => sb.append(row.getAs[Int]("age")).append(" ")
        case "ChildName" => sb.append(row.getAs[String]("name")).append(" ")
      }

      map(1) match {
        case "ChildState" => sb.append(row.getAs[String]("state")).append(" ")
        case "ChildAge" => sb.append(row.getAs[Int]("age")).append(" ")
        case "ChildName" => sb.append(row.getAs[String]("name")).append(" ")
      }

      map(2) match {
        case "ChildState" => sb.append(row.getAs[String]("state")).append(" ")
        case "ChildAge" => sb.append(row.getAs[Int]("age")).append(" ")
        case "ChildName" => sb.append(row.getAs[String]("name")).append(" ")
      }

      Row(row.getAs[String]("name"), row.getAs[Int]("age"), row.getAs[String]("state"), sb.toString())
    }) (encoder)
    res.show(false)
//
//    val child_state_pos = array_position(split(regexp_replace($"description", "\\s+", ""), "\\+"), "ChildState")
//    data
//      .withColumn("abc",
//        when(child_state_pos === 1, $"state").otherwise("$name")
//      )
//      .show()
  }

}
