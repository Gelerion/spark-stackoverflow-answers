package com.gelerion.spark.custom.intuit

import java.time.LocalDate

object Main {

  def main(args: Array[String]): Unit = {
    val processingDate = LocalDate.parse("2019-12-25")
    val periods = Array(1, 3) /*.map(Period.ofDays)*/
    for (period <- periods) {
      println(Stream.range(0, period).map(days => processingDate.minusDays(days)).toSet)
//      println(Stream.continually({
//        val previous = LocalDate.parse("2019-12-25").minusDays(1)
//        previous
//      })
//        .take(3).toSet)
    }
  }

}
