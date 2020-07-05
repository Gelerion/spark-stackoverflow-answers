package com.gelerion.spark.so.batch.windows

import com.gelerion.spark.Spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.Random

object ParallelismOfWindows extends Spark {
  import spark.implicits._

  //https://stackoverflow.com/questions/58146383/spark-fails-to-repartition-after-window-partitionby
  def main(args: Array[String]): Unit = {
    /*
    1. Sort elements by SCORE within a GROUP and compute cumulative sum of each element.
    2. Assign Bucket to each SCORE within a GROUP, using get_bucket_udf UDF.
    3. Create structure {'GROUP_ID', 'BUCKET', 'SCORE'} (SCORE_SET) for each element and aggregate those structures it into a list for each element.

    * GROUP_ID - there are only 3 of them: 1, 2, 3
    * ELEMENT_ID - there are about 200M of them

    Before step 1 I partition by GROUP_ID and, therefore, steps 1-2 are run on only 3 executors, since we only have 3 GROUPs.

    Right after step 2 I try to repartition by ELEMENT_ID to take advantage of 100 executors, but by looking at
    Hadoop Resource Manager I am seeing that it still uses only 3 executors.
     */

    val df = genData()

    val groupIdPartition = Window.partitionBy("group_id")
    val groupIdOrderByScore = groupIdPartition.orderBy("score")
    val groupIdOrderByRank = groupIdPartition.orderBy("score_rank")

    val groupIdScoreRanked = df.select($"*", row_number().over(groupIdOrderByScore).as("score_rank"))
    groupIdScoreRanked.explain(true)
    //groupIdScoreRanked.show()

    //Compute cumulative sum for each row within group_id
    val dfEnhanced = groupIdScoreRanked.select(
      $"group_id",
      $"element_id",
      $"score",
      sum($"score").over(groupIdPartition).alias("score_sum"),
      sum("score").over(groupIdOrderByRank.rangeBetween(Window.unboundedPreceding, Window.currentRow)).alias("score_cum_sum")
    ).orderBy("group_id", "score")

//    dfEnhanced.explain(true)
//    dfEnhanced.show(300, false)

    // Step 2
    val getBucketUdf = spark.udf.register("get_bucket", (sum: Int, cumSum: Int) => 1 +  new Random().nextInt(10))
    val dfEnhancedBucketed = dfEnhanced.select($"*", getBucketUdf($"score_sum", $"score_cum_sum").alias("bucket"))
    println(s"dfEnhancedBucketed: ${dfEnhancedBucketed.rdd.getNumPartitions}") //128

    val dfEnhancedBucketedRepartitioned = dfEnhancedBucketed.repartition($"element_id")
    println(s"dfEnhancedBucketedRepartitioned: ${dfEnhancedBucketedRepartitioned.rdd.getNumPartitions}") //200 -> default parallelism

    // Step 3
    val outputDfStructed = dfEnhancedBucketedRepartitioned
      .select($"element_id", struct($"group_id", $"bucket", $"score").alias("score_set"))
    println(s"outputDfStructed: ${outputDfStructed.rdd.getNumPartitions}")

    val result = outputDfStructed.groupBy("element_id")
      .agg(collect_list($"score_set").alias("score_set"))
    println(s"result: ${result.rdd.getNumPartitions}")


    result.collect()
  }


  def genData(): DataFrame = {
    //group_id, element_id, score
    val rnd = new Random()
    Stream.continually((1 + rnd.nextInt(3), rnd.nextInt(1000/*000*/), 1 + rnd.nextInt(50)))
      .take(300)
      .force
      .toDF("group_id", "element_id", "score")
  }

}
