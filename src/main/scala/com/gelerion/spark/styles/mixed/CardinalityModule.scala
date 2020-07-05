package com.gelerion.spark.styles.mixed

import com.gelerion.spark.styles.{Field, LoadFieldSpec, LoadRangeSpec, Strategies}
import org.apache.spark.sql.DataFrame

// this module has the bare minimum, all the functional logic is under cardinality "namespace"
trait CardinalityModule {
  val loadGrayLists: Seq[LoadFieldSpec] => DataFrame

  def applyCardinality(fields: Seq[Field], loadSpec: LoadRangeSpec)(baseDf: DataFrame): DataFrame = {
    val applyCardinalityFn = cardinality.applyCardinality(fields, loadSpec, Strategies.hashingStrategy)(loadGrayLists)
    baseDf.transform(applyCardinalityFn)
  }
}

