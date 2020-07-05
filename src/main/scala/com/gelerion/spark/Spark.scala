package com.gelerion.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Spark {
  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("af-so")
    .master("local[*]")
    .config(new SparkConf()
        .set("spark.sql.hive.thriftServer.singleSession", "true")
        .set("hive.server2.thrift.port", "10001")
        .set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=metastore_db2;create=true")
    )
    .getOrCreate()

}
