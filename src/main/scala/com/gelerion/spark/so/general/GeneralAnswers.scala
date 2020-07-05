package com.gelerion.spark.so.general

import java.sql.Date
import java.time.format.DateTimeFormatter

import com.gelerion.spark.Spark
import com.gelerion.spark.so.conditions.SplitValuesDynamically.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, MapType, StringType, StructField, StructType}
import org.joda.time.Weeks
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag


object GeneralAnswers extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    //createValuePerExec()

    //explodingMaps()

    //filterOptionMonad()

    //typedLiterals()

    //invalidDateToCurrent()

//    flattenNestedArray()

    //rowWiseMaxValue()

//    cubeExample()

//    mappingArrays()

//    isoTimeFormat()

//    bigDecimalIssue()

//    updateMapValues()

//    weeksRange()

    custom3()
  }

  def custom3() = {
    val dfSchema = StructType(
      StructField("ROWNUM", StringType, false)
        :: StructField("SOME_INT", StringType, true)
        :: StructField("SOME_STRING", StringType, false)
        :: Nil)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("nullValue", "NA")
      .schema(dfSchema)
      .load("src/main/resources/csv_with_nan.csv")

    //Equality test that is safe for null values. - EqualNullSafe
    val maybeFilter = Option.empty

    maybeFilter.getOrElse("true")

    val where = df.where(maybeFilter.getOrElse("true"))
    where.explain(true)
    where.show()
  }

  def custom2(): Unit = {
//    ROWNUM,SOME_INT,SOME_STRING
//    1     ,12      ,aaa
//    2     ,NA      ,bbb
//    4     ,NA      ,ddd
//    3     ,32      ,ccc
//    */

    val dfSchema = StructType(
      StructField("ROWNUM", StringType, false)
        :: StructField("SOME_INT", StringType, true)
        :: StructField("SOME_STRING", StringType, false)
        :: Nil)


    val df = spark.read.format("csv")
      .option("header", "true")
      .option("nullValue", "NA")
      .schema(dfSchema)
      .load("src/main/resources/csv_with_nan.csv")

    //Equality test that is safe for null values. - EqualNullSafe
    val where = df.where(!($"SOME_INT" <=> 12))
    where.count()
    where.show()

  }

  def custom(): Unit = {
//    val example = Seq(
//      Grade(1,3,1,1),
//      Grade(1,1,null,1),
//      Grade(1,10,2,1)
//    )

    /*
    ROWNUM,SOME_INT,SOME_STRING
    1     ,12      ,aaa
    2     ,NA      ,bbb
    4     ,NA      ,ddd
    3     ,32      ,ccc
     */
    val dfSchema = StructType(
      StructField("ROWNUM", IntegerType, true)
        :: StructField("SOME_INT", IntegerType, false)
        :: StructField("SOME_STRING", StringType, false)
        :: Nil)


    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(dfSchema)
      .load("src/main/resources/csv_with_nan.csv")

//    df.head(3).foreach(println)

    val dfNullRows = df.filter(df.col("ROWNUM").isNull)
//    dfNullRows.explain(true)
//    dfNullRows.columns.foreach(println)
//    dfNullRows.select("ROWNUM", "SOME_STRING").head(3).foreach(println)

//    dfNullRows.groupBy("ROWNUM", "SOME_INT").count().explain(true)

//    dfNullRows.groupBy("ROWNUM", "SOME_STRING", "SOME_INT").count().explain(true)
//    dfNullRows.groupBy("ROWNUM", "SOME_STRING", "SOME_INT").count().show()

//    dfNullRows.groupBy().count().explain(true)
//    df.groupBy().count().show()
//    df.groupBy("ROWNUM", "SOME_STRING", "SOME_INT").count().show()

    //-------- Explain
    println("Explain for dfNullRows.count()")
    val count = dfNullRows.groupBy().count()
    count.explain()

    println("--------------------------------------")
    println("Explain for dfNullRows.groupBy(\"ROWNUM\", \"SOME_STRING\", \"SOME_INT\").count()")
    dfNullRows.groupBy("ROWNUM", "SOME_STRING", "SOME_INT").count().explain(true)


    dfNullRows.select($"a" =!= "a")
    //Count under the hood
//    println(s"Manually written count: ${dfNullRows.groupBy().count().collect().head.getLong(0)}")

    //works as expected, count is 2
    println(s"Explicitly written count: " +
      s"${dfNullRows.groupBy("ROWNUM", "SOME_STRING", "SOME_INT").count().collect().head.getAs[Long]("count")}")

//    println(dfNullRows.count())
//    println(dfNullRows/*.select("ROWNUM", "SOME_STRING")*/.show())
  }

  //Spark - Find the range of all year weeks between 2 weeks
  //https://stackoverflow.com/questions/58015618/spark-find-the-range-of-all-year-weeks-between-2-weeks
  def weeksRange(): Unit = {
    //week days
    val dates = Seq("201849 - 201903", "201912 - 201917").toDF("col")

     def weeksSinceInstall(weeksRange: String): Array[String] = {
      try {
        val left =  weeksRange.split("-")(0).trim
        val right = weeksRange.split("-")(1).trim

        val leftPattern  = s"${left.substring(0, 4)}-${left.substring(4)}"
        val rightPattern = s"${right.substring(0, 4)}-${right.substring(4)}"

        val fmt = DateTimeFormat.forPattern("yyyy-w")

        val leftDate  = fmt.parseDateTime(leftPattern)
        val rightDate = fmt.parseDateTime(rightPattern)
        //if (leftDate.isAfter(rightDate))
        val weeksBetween = Weeks.weeksBetween(leftDate, rightDate).getWeeks
        val dates = for (one <- 0 to weeksBetween) yield {
          leftDate.plusWeeks(one)
        }

        val result: Array[String] = dates.map(date => fmt.print(date)).map(_.replaceAll("-", "")).toArray
        result
      } catch {
        case e: Exception => Array.empty
      }
    }

    val weeks = udf((weeks: String) => weeksSinceInstall(weeks))

    dates.select(weeks($"col"))
      .show(false)
  }

  def updateMapValues(): Unit = {
    val ds = Seq(
      ("1ABC", "101ATP"),
      ("2BCA", "ZER987")
    ).toDF("col_a", "col_b")


    val sortUdf = spark.udf.register("sort", (value: String) => value.sorted)
    ds.select(sortUdf($"col_a").as("col_a"), sortUdf($"col_b").as("col_b"))
      .show(false)

//    ds.select(spli)
  }

  //Spark function avg and BigDecimal's scale issue
  def bigDecimalIssue(): Unit = {
    val data: Dataset[AllData] = List(
      AllData(1, "w1", "p1", BigDecimal("1.1111111"), 10),
      AllData(2, "w2", "p2", BigDecimal("2.1111111111"), 10),
      AllData(2, "w1", "p1", BigDecimal("3.11111111"), 10)
    ).toDS()

    val statisticForAmounts = data.groupByKey(record => record.warehouse + ", " + record.product)
      .agg(
        max($"amount").as("maxAmount").as[BigDecimal],
        avg($"amount").as("avgAmount").as[BigDecimal]
      )

    statisticForAmounts.printSchema()
    statisticForAmounts.show()
  }

  case class AllData(positionId: Long, warehouse: String, product: String, amount: BigDecimal, amountTime: Long)

  def isoTimeFormat(): Unit = {
    val timestamped = List("1970-01-01 00:00:00.0").toDF("timestr")
    timestamped.select(date_format($"timestr", "yyyy-MM-dd'T'HH:mm:ss.SS'Z'"))
      .show(false)

  }

  //https://stackoverflow.com/questions/57752674/in-spark-according-to-the-mapping-table-is-there-any-way-to-convert-an-array-of
  def mappingArrays():Unit = {
    val sampleDataDs = Seq(
      ("a",25,Map(1->SampleClass(1,2))),
      ("a",32,Map(1->SampleClass(3,4), 2->SampleClass(1,2))),
      ("b",16,Map(1->SampleClass(1,2))),
      ("b",18,Map(2->SampleClass(10,15))))
      .toDF("letter","number","maps")

    val typedDs: Dataset[(String, Int, Map[Int, SampleClass])] = Seq(
      ("a",25,Map(1->SampleClass(1,2))),
      ("a",32,Map(1->SampleClass(3,4), 2->SampleClass(1,2))),
      ("b",16,Map(1->SampleClass(1,2))),
      ("b",18,Map(2->SampleClass(10,15)))).toDS()

//    typedDs.printSchema()
//    typedDs.show()


//    typedDs
//      .groupBy("_1")
//      .agg(collect_list(typedDs("_3")).alias("mapList"))
//      .show()

    val aggregatedDs = sampleDataDs
      .groupBy("letter")
      .agg(collect_list(sampleDataDs("maps")).alias("mapList"))

    val value: Dataset[(String, Array[Map[Int, SampleClass]])] = aggregatedDs.as[(String, Array[Map[Int, SampleClass]])]
    value.printSchema()
    value.show(false)

//    aggregatedDs.printSchema()
//    aggregatedDs.show(false)

    def mergeMap = udf { valSeq:Array[Map[Int,SampleClass]] =>
      valSeq.flatten.groupBy(_._1).mapValues(x=>(x.map(_._2.value1).sum,x.map(_._2.value2).sum))
    }

    def mergeMap1 = udf { valSeq:Array[Map[Int,SampleClass]] =>
      value
    }

    val mapped: Dataset[(String, Map[Int, (Int, Int)])] = value.map(v => {
      (v._1, v._2.flatten.groupBy(_._1).mapValues(x=>(x.map(_._2.value1).sum,x.map(_._2.value2).sum)))
    })

    mapped.show(false)


    value.withColumn("aggValues", mergeMap1(col("mapList"))).show()
  }

  case class SampleClass(value1:Int,value2:Int)


  def cubeExample(): Unit = {
    val ds = Seq(
      ("dep1", "M", 1200, 34),
      ("dep1", "M", 800, 30),
      ("dep1", "F", 200, 21),
      ("dep2", "M", 1000, 21),
      ("dep2", "M", 1200, 22),
      ("dep2", "F", 500, 24),
      ("dep2", "M", 600, 44)
    ).toDF("department", "gender", "salary", "age")

    val dfWithDate = Seq(
      (17850.0, "85123A", 6, 2.55),
      (17850.0, "71053", 6, 1.55),
      (17850.0, "84406B", 8, 3.55)
    ).toDF("CustomerId", "StockCode", "Quantity", "UnitPrice")

    dfWithDate.cube("CustomerId", "StockCode").agg(grouping_id(), sum("Quantity")).orderBy(expr("grouping_id()").desc)
      .show()

//    ds.cube($"department", $"gender").agg(Map(
//           "salary" -> "avg",
//           "age" -> "max"
//       ))
    ds.cube($"department", $"gender").agg(
      avg($"salary"), max($"age"), grouping_id()
    )
//      .orderBy($"gid".desc)
      .orderBy(expr("grouping_id()").desc)
      .show(false)
  }

  //https://stackoverflow.com/questions/57707514/scala-dataframe-get-max-value-of-specific-row
  def rowWiseMaxValue(): Unit = {
    val tmp= Seq((0.1,0.3, 0.4), (0.3, 0.1, 0.4), (0.2, 0.2, 0.5)).toDF("a", "b", "c")
    val top = tmp.filter(col("a") === 0.1)
    /*
    +---+---+---+
    |  a|  b|  c|
    +---+---+---+
    |0.1|0.3|0.4|
    +---+---+---+

    Desired output if i want top 2 max
    +---+---
    |  b|c  |
    +---+--+
    |0.3|0.4|
    +---+---

    Desired output if i want min
    +---+
    |  a|
    +---+
    |0.1|
    +---+
     */

    val max = top.map(row => maxN(row.getValuesMap(Seq("a", "b", "c")), 2)).toDF()

    max.show()
    max.select(explode($"value")).show()

    /*
    +---+-----+
    |key|value|
    +---+-----+
    |  a|  0.1|
    |  b|  0.3|
    +---+-----+
     */

    max.select(explode($"value"))
      .withColumn("idx", lit(1))
      .groupBy($"idx").pivot($"key").agg(first($"value"))
      .drop("idx")
      .show()

    val minVal = top.map(row => {
      min(row.getValuesMap(Seq("a", "b", "c")))
    }).toDF()

    minVal.select(explode($"value"))
      .withColumn("idx", lit(1))
      .groupBy($"idx").pivot($"key").agg(first($"value"))
      .drop("idx")
      .show()

  }

  def maxN(values: Map[String, Double], n: Int): Map[String, Double] = {
    values.toSeq.sorted.reverse.take(2).toMap
  }

  def min(values: Map[String, Double]): Map[String, Double] = {
    Map(values.toSeq.min)
  }

  def flattenNestedArray(): Unit = {
    val df = spark.read.json("src/main/resources/nestedJson.txt")
    df.createOrReplaceTempView("people1")
    df.show()

    df.selectExpr("filter(info.privateInfo.salary, salary -> salary is not null) as salary").show()

    df.printSchema()
    //https://docs.databricks.com/_static/notebooks/apache-spark-2.4-functions.html
    // **null can never be equal to null.
    //df.select($"name", array_remove($"info.privateInfo.salary", null)).show()

    //won't work
    //Only one generator allowed per select clause but found 2
    df.selectExpr(
      "explode(filter(info.privateInfo.salary, salary -> salary is not null)) AS salary",
      "explode(filter(info.privateInfo.sex, sex -> sex is not null)) AS sex"
    )
      .show()

    df.selectExpr(
      "element_at(filter(info.privateInfo.salary, salary -> salary is not null), 1) AS salary",
      "element_at(filter(info.privateInfo.sex, sex -> sex is not null), 1) AS sex"
    )
      .show()

    val sqlDF = spark.sql("SELECT name , " +
      "element_at(filter(info.privateInfo.salary, salary -> salary is not null), 1) AS salary ," +
      "element_at(filter(info.privateInfo.sex, sex -> sex is not null), 1) AS sex" +
      "   FROM people1 ")
//    val sqlDF = spark.sql("SELECT name , info.privateInfo.salary ,info.privateInfo.sex   FROM people1 ")
    sqlDF.show()
//    df.show()
  }

  //https://stackoverflow.com/questions/57554639/validate-time-stamp-in-input-spark-dataframe-to-generate-correct-output-spark-da
  def invalidDateToCurrent(): Unit = {
    val fmt = "yyyy-MM-dd HH:mm:ss"

    val df = Seq(
      "2019-10-21 14:45:23",
      "2019-10-22 14:45:23",
      null,
      "2019-10-41 14:45:23"
    ).toDF("ts")

//    df.printSchema()
    df.withColumn("ts", to_timestamp($"ts", fmt))
      .withColumn("ts", when($"ts".isNull, date_format(current_timestamp(), fmt)).otherwise($"ts"))
      .show(false)
  }

  def typedLiterals(): Unit = {
    val values = List(1,2,3,4,5)

    val df = values.toDF("id")
    var t2 = Tuple2(1,2)
    val df2: DataFrame = df.withColumn("qq", typedLit(t2))


    val r1 = df2.head()
    //    qq.asInstanceOf[scala.Tuple2[Int,Int]]
  }

  def filterOptionMonad(): Unit = {
    val ds = List(
      BinaryHolder("abc".getBytes()),
      BinaryHolder("dbe".getBytes()),
      BinaryHolder("aws".getBytes()),
      BinaryHolder("qwe".getBytes())
    ).toDS()

    val df: DataFrame = ds.toDF()
    df.select($"value".as[Array[Byte]])
      .map(binaryData => {
        BinaryHolder(binaryData).toCaseClassMonad() match {
          case Some(value) => Tuple1(value)
          case None => Tuple1(null)
        }
      })
      .filter(tuple => tuple._1 != null)
      .map(tuple => tuple._1)
      .show()

//    val typ: Dataset[MyString] = df.select($"value".as[Array[Byte]])
//      .flatMap(binaryData => {
//        BinaryHolder(binaryData).toCaseClassMonad() match {
//          case Some(value) => Seq(value)
//          case None => Seq()
//        }
//      })
//    val filter = ds.flatMap(a => {
//      val opt = if (a.name == "A") Some(a) else None
//      opt match {
//        case Some(v) => v
//        case None => null
//      }
//    })



//    val jobEventDS = spark.emptyDataset
//      .select($"value".as[Array[Byte]])
//      .map(binaryData => {
//        FromThrift.ToJobEvent(binaryData) match {
//          case Some(v) => v
//          case None => null
//        }
//      })
//      .filter(MaybeJobEvent => MaybeJobEvent match {
//        case Some(_) => true
//        case None => false
//      }).map {
//      case Some(jobEvent) => jobEvent
//      case None => null
//    }

//    xfilter.show(false)
  }

  //https://stackoverflow.com/questions/57435607/unable-to-explode-mapstring-struct-in-spark
  def explodingMaps(): Unit = {
    //input
    //(country: Sweden, carMake: Volvo, carMake.Models: {"850": ("T5", "petrol"), "V50": ("T5", "petrol")})

    //output
    //(country: Sweden, carMake: Volvo, Model: "850", Variant: "T5", GasOrPetrol: "petrol"}
    //(country: Sweden, carMake: Volvo, Model: "V50", Variant: "T5", GasOrPetrol: "petrol"}

    //explode($"carmake.models")

    val df = List(
      MyRow(CarMake("volvo",Map(
        "850" -> Models("T5","petrol"),
        "V50" -> Models("T5","petrol")
      )))
    ).toDF()

//    df.show(false)

    /*
    +---------------------------------------------------+
    |carMake                                            |
    +---------------------------------------------------+
    |[volvo, [850 -> [T5, petrol], V50 -> [T5, petrol]]]|
    +---------------------------------------------------+
     */

    /*
     root
     |-- carMake: struct (nullable = true)
     |    |-- brand: string (nullable = true)
     |    |-- models: map (nullable = true)
     |    |    |-- key: string
     |    |    |-- value: struct (valueContainsNull = true)
     |    |    |    |-- variant: string (nullable = true)
     |    |    |    |-- gasOrPetrol: string (nullable = true)
     */

    val cols: Array[Column] = df.columns.map(col)

    //now explode, note that withColumn does not work because explode on a map
    //returns 2 columns (key and value), so you need to use select:
//    df.withColumn("exp", explode($"carMake.models")).show()

    val exploded = df.select(cols :+ explode($"carMake.models"):_*)
    /*
    +---------------------------------------------------+---+------------+
    |carMake                                            |key|value       |
    +---------------------------------------------------+---+------------+
    |[volvo, [850 -> [T5, petrol], V50 -> [T5, petrol]]]|850|[T5, petrol]|
    |[volvo, [850 -> [T5, petrol], V50 -> [T5, petrol]]]|V50|[T5, petrol]|
    +---------------------------------------------------+---+------------+
     */

    exploded.select(cols :+ $"key".as("model") :+ $"value.*" :_*)
      .show(false)
  }


  //https://stackoverflow.com/questions/57378174/does-the-following-create-a-new-object-on-every-executor-for-every-row-in-spark/57378595#57378595
  def createValuePerExec(): Unit = {
//    val a = List(1,2,3).toDF
//    val df = a.filter(col("value") === new AddCmplx().getVal)
//    df.explain(true)
//    df.show()

    val b = List(A("ME")).toDS
    val df1 = b.filter(l => new Eq().isMe(l.name))
    df1.explain(true)
    df1.show()
  }
}

case class Add(value: Int) { def getVal: Int = value }
class AddCmplx() { val adding = 2; def getVal: Add = new Add(2)}
case class A(name:String)
class Eq { def isMe(s:String) = s == "ME" }

case class Models(variant:String, gasOrPetrol:String)
case class CarMake(brand:String, models : Map[String, Models] )
case class MyRow(carMake:CarMake)
case class BinaryHolder(value: Array[Byte]) {
  def toStrMonad(): Option[String] = new String(value) match {
    case "abc" => None
    case _ => Some(new String(value))
  }

  def toCaseClassMonad(): Option[MyString] = new String(value) match {
    case "abc" => None
    case _ => Some(MyString(new String(value)))
  }

  def toMyStrMonad(): Option[MyStringNotProduct] = new String(value) match {
    case "abc" => None
    case _ => Some(new MyStringNotProduct(new String(value)))
  }
}

case class MyString(str: String)
class MyStringNotProduct(str: String)
case class Grade(c1: Integer, c2: Integer, c3: Integer, c4: Integer) {
  def dropWhenNotEqualsTo(n: Integer): Grade = {
    Grade(nullOrValue(c1, n), nullOrValue(c2, n), nullOrValue(c3, n), nullOrValue(c4, n))
  }
  def nullOrValue(c: Integer, n: Integer) = if (c == n) c else null
}
