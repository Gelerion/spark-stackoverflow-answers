package com.gelerion.spark.streaming.book.output_modes

object StreamingOutputModes {

  /**
   * append
   * (default mode) Adds only final records to the output stream. A record is considered final when no new records of
   * the incoming stream can modify its value. This is always the case with linear transformations like those resulting
   * from applying projection, filtering, and mapping. This mode guarantees that each resulting record will be output only once.
   *
   * update
   * Adds new and updated records since the last trigger to the output stream. update is meaningful only in the context
   * of an aggregation, where aggregated values change as new records arrive. If more than one incoming record changes
   * a single result, all changes between trigger intervals are collated into one output record.
   *
   * complete
   * complete mode outputs the complete internal representation of the stream. This mode also relates to aggregations,
   * because for nonaggregated streams, we would need to remember all records seen so far, which is unrealistic.
   * From a practical perspective, complete mode is recommended only when you are aggregating values over low-cardinality
   * criteria, like count of visitors by country, for which we know that the number of countries is bounded.
   */

  /*
  UNDERSTANDING THE APPEND SEMANTIC

  When the streaming query contains aggregations, the definition of final becomes nontrivial. In an aggregated computation,
  new incoming records might change an existing aggregated value when they comply with the aggregation criteria used.
  Following our definition, we cannot output a record using append until we know that its value is final. Therefore,
  the use of the append output mode in combination with aggregate queries is restricted to queries for which the
  aggregation is expressed using event-time and it defines a watermark. In that case, append will output an event
  as soon as the watermark has expired and hence it’s considered that no new records can alter the aggregated value.
  As a consequence, output events in append mode will be delayed by the aggregation time window plus the watermark offset.
   */
}
