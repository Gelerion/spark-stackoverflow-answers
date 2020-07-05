package com.gelerion.spark.so.conditions

import com.gelerion.spark.Spark
import org.apache.spark.sql.functions._

object FilterConditionAsString extends Spark {

  def main(args: Array[String]): Unit = {
    import spark.implicits._

//    val data = Seq(
//      (1, "ABC", "ADI", "GUJ"),
//      (2, "BCD", null, "MAH"),
//      (3, null, "ADI", "GUJ"),
//      (4, "ABC", "ADI", "DEL"),
//      (null, null, "PUN", null),
//      (6, "DSF", "MUM", "MAH"),
//      (7, "DSFDFS", null, "RAJ"),
//      (8, null, "PUN", "MAH"),
//      (9, "FDA", "JAI", "RAJ"),
//      (10, "DFAD", null, "GUJ"),
//      (11, null, null, null),
//      (12, null, null, null)
//    ).toDF("id", "name", "city", "state")

    val data = Seq(
      ("1", "ABC", "ADI", "GUJ"),
      ("2", "BCD", null, "MAH"),
      ("3", null, "ADI", "GUJ"),
      ("4", "ABC", "ADI", "DEL"),
      (null, null, "PUN", null),
      ("6", "DSF", "MUM", "MAH"),
      ("7", "DSFDFS", null, "RAJ"),
      ("8", null, "PUN", "MAH"),
      ("9", "FDA", "JAI", "RAJ"),
      ("10", "DFAD", null, "GUJ"),
      ("11", null, null, null),
      ("12", null, null, null)
    ).toDF("id", "name", "city", "state")

    data
//      .filter(col("State").isNull && (col("City").isNull || col("Name").isNull))
      .filter("State IS NULL AND (City IS NULL OR Name IS NULL)")
      .show()
   
    
    
  }
}
