package com.gelerion.spark.so.conditions

import com.gelerion.spark.Spark
import com.gelerion.spark.so.conditions.FilterConditionAsString.spark
import org.apache.spark.sql.functions._

object SplitValuesDynamically extends Spark {

  //https://stackoverflow.com/questions/57351806/split-values-in-dataframe-into-separate-dataframe-column-spark-scala
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    //Split values in dataframe into separate dataframe column -spark scala
    val df = Seq(
      (1, "name=John || age=25 || col1 =val1  || col2= val2"),
      (2, "name=Joe  || age=23 || col1 =val11 || col2= val22")
    ).toDF("id", "vals")

    df
      .withColumn("flattened", explode(split($"vals", "\\s*\\|\\|\\s*")))
    /*
    +---+-------------------------------------------------+-----------+
    |id |vals                                             |flattened  |
    +---+-------------------------------------------------+-----------+
    |1  |name=John || age=25 || col1 =val1  || col2= val2 |name=John  |
    |1  |name=John || age=25 || col1 =val1  || col2= val2 |age=25     |
    |1  |name=John || age=25 || col1 =val1  || col2= val2 |col1 =val1 |
    |1  |name=John || age=25 || col1 =val1  || col2= val2 |col2= val2 |
    |2  |name=Joe  || age=23 || col1 =val11 || col2= val22|name=Joe   |
    |2  |name=Joe  || age=23 || col1 =val11 || col2= val22|age=23     |
    |2  |name=Joe  || age=23 || col1 =val11 || col2= val22|col1 =val11|
    |2  |name=Joe  || age=23 || col1 =val11 || col2= val22|col2= val22|
    +---+-------------------------------------------------+-----------+
     */
      .withColumn("kv_array", split($"flattened", "\\s*=\\s*"))
      /*
      +---+-------------------------------------------------+-----------+-------------+
      |id |vals                                             |flattened  |kv_array     |
      +---+-------------------------------------------------+-----------+-------------+
      |1  |name=John || age=25 || col1 =val1  || col2= val2 |name=John  |[name, John] |
      |1  |name=John || age=25 || col1 =val1  || col2= val2 |age=25     |[age, 25]    |
      |1  |name=John || age=25 || col1 =val1  || col2= val2 |col1 =val1 |[col1, val1] |
      |1  |name=John || age=25 || col1 =val1  || col2= val2 |col2= val2 |[col2, val2] |
      |2  |name=Joe  || age=23 || col1 =val11 || col2= val22|name=Joe   |[name, Joe]  |
      |2  |name=Joe  || age=23 || col1 =val11 || col2= val22|age=23     |[age, 23]    |
      |2  |name=Joe  || age=23 || col1 =val11 || col2= val22|col1 =val11|[col1, val11]|
      |2  |name=Joe  || age=23 || col1 =val11 || col2= val22|col2= val22|[col2, val22]|
      +---+-------------------------------------------------+-----------+-------------+
       */
      .groupBy("id").pivot($"kv_array"(0)).agg(first($"kv_array"(1)))
      .show(false)
    /*
    +---+---+-----+-----+----+
    |id |age|col1 |col2 |name|
    +---+---+-----+-----+----+
    |1  |25 |val1 |val2 |John|
    |2  |23 |val11|val22|Joe |
    +---+---+-----+-----+----+
     */
  }

}
