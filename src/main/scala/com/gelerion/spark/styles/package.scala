package com.gelerion.spark

package object styles {
  //Functional Spark
  // 1. Using frameless -> https://github.com/typelevel/frameless
  //    + Typesafe columns referencing (e.g., no more runtime errors when accessing non-existing columns)
  //    + Enhanced type signature for built-in functions (e.g., if you apply an arithmetic operation on a non-numeric column, you get a compilation error)
  //    + Typesafe casting and projections
  //    - Ser to Dataset
}
