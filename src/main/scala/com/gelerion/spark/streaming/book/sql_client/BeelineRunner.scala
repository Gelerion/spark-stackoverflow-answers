package com.gelerion.spark.streaming.book.sql_client

import org.apache.hive.beeline.BeeLine

object BeelineRunner {

  def main(args: Array[String]): Unit = {
//    BeeLine.main(Array("-u jdbc:hive2://localhost:10001", "-n usr", "-p pass", "-e SHOW TABLES;"))
    BeeLine.main(Array("-u jdbc:hive2://localhost:10001", "-n usr", "-p pass", "-e select * from urlranks limit 100"))
  }

}
