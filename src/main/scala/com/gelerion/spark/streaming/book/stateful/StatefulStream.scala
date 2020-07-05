package com.gelerion.spark.streaming.book.stateful

import java.sql.Timestamp

import com.gelerion.spark.Spark
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import scala.collection.immutable.Queue
import scala.util.Random

object StatefulStream extends Spark {
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // suppose that our requirement is to compute a moving average of the last 10 elements received per key
    val raw = spark.readStream
      .format("rate")
//      .option("recordsPerSecond", 5)
      .load().as[Rate]

    val weatherEvents = raw.map{ case Rate(ts, value) => WeatherEvent.generate(ts) }

    val weatherEventsMovingAverage = weatherEvents.groupByKey(record => record.stationId)
      // Besides the mapping function, we also need to provide a timeout type. A timeout type
      // can be either a ProcessingTimeTimeout or an EventTimeTimeout. Because we are not relying on the timestamp
      // of the events for our state management, we chose the ProcessingTimeTimeout.

      //.mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mappingFunction)

      // eliminating zero values
      // Using flatMapGroupsWithState, we no longer need to produce artificial zeroed records. In addition to that,
      // our state management definition is now strict in having n elements to produce a result
      .flatMapGroupsWithState(OutputMode.Update, GroupStateTimeout.ProcessingTimeTimeout)(flatMappingFunctionV2)

    /*
    flat-map modes:

    update
     This indicates that the records produced are nonfinal. They are intermediate results that might be updated with
     new information later on. In the previous example, the arrival of new data for a key will produce a new data point.
     The downstream sink must use update and no aggregations can follow the flatMapGroupsWithState operation.

    append
     This designates that we have collected all of the information we need to produce a result for a group, and no further
     incoming events will change that outcome. The downstream sink must use append mode to write. Given that the
     application of flatMapGroupsWithState produces a final record, it’s possible to apply further aggregations to that result.
     */

    val outQuery = weatherEventsMovingAverage.writeStream
      .format("console")
      .outputMode("update")
      .start()

    outQuery.awaitTermination()

    /*
    Managing state operations

     Structured Streaming exposes time and timeout information that we can use to decide when to expire certain state.
     The first step is to decide the time reference to use. Timeouts can be based on event time or processing time and
     the choice is global to the state handled by the particular [map|flatMap]GroupsWithState being configured

     To use event time, we also need to declare a watermark definition. This definition consists of the timestamp field
     from the event and the configured lag of the watermark. If we wanted to use event time with the previous example,
     we would declare it like so:

     weatherEvents
      .withWatermark("timestamp", "2 minutes")
      .groupByKey(record => record.stationId)
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout)(mappingFunction)


     */
  }

  def mappingFunction(key: String, values: Iterator[WeatherEvent], state: GroupState[FIFOBuffer[WeatherEvent]]): WeatherEventAverage = {
    // the size of the window
    val eliminationCountWindowSize = 10

    // get current state or create a new one if there's no previous state
    val currentState = state.getOption.getOrElse(new FIFOBuffer[WeatherEvent](eliminationCountWindowSize))

    // enrich the state with the new events
    val updatedState = values.foldLeft(currentState) {
      case (state, event) => state.add(event)
    }

    // update the state with the enriched state
    state.update(updatedState)

    // if we have enough data, create a WeatherEventAverage from the state otherwise, make a zeroed record
    val data = updatedState.get
    if (data.size > 2) {
      val start: WeatherEvent = data.head
      val end: WeatherEvent = data.last
      val pressureAvg = data.map(event => event.pressure).sum / data.size
      val tempAvg = data.map(event => event.temp).sum / data.size
      WeatherEventAverage(key, start.timestamp, end.timestamp, pressureAvg, tempAvg)
    } else {
      WeatherEventAverage.empty(key)
    }
  }

  //No state expiration management!
  def flatMappingFunction(key: String, values: Iterator[WeatherEvent], state: GroupState[FIFOBuffer[WeatherEvent]]): Iterator[WeatherEventAverage] = {
    // the size of the window
    val eliminationCountWindowSize = 10

    // get current state or create a new one if there's no previous state
    val currentState = state.getOption.getOrElse(new FIFOBuffer[WeatherEvent](eliminationCountWindowSize))

    // enrich the state with the new events
    val updatedState = values.foldLeft(currentState) {
      case (state, event) => state.add(event)
    }

    // update the state with the enriched state
    state.update(updatedState)

    // if we have enough data, create a WeatherEventAverage from the state otherwise, make a zeroed record
    val data = updatedState.get
    if (data.size == eliminationCountWindowSize) {
      val start: WeatherEvent = data.head
      val end: WeatherEvent = data.last
      val pressureAvg = data.map(event => event.pressure).sum / data.size
      val tempAvg = data.map(event => event.temp).sum / data.size
      Iterator(WeatherEventAverage(key, start.timestamp, end.timestamp, pressureAvg, tempAvg))
    } else {
      Iterator.empty
    }
  }

  // the size of the window
  val eliminationCountWindowSize = 10
  def flatMappingFunctionV2(key: String, values: Iterator[WeatherEvent], state: GroupState[FIFOBuffer[WeatherEvent]]): Iterator[WeatherEventAverage] = {
    // first check for timeout in the state
    if (state.hasTimedOut) {
      // when the state has a timeout, the values are empty this validation is only to illustrate the point
      assert(values.isEmpty, "When the state has a timeout, the values are empty")

      val result = stateToAverageEvent(key, state.get)
      // evict the timed-out state
      state.remove()

      // emit the result of transforming the current state into an output record
      return result
    }

    // get current state or create a new one if there's no previous state
    val currentState = state.getOption.getOrElse(new FIFOBuffer[WeatherEvent](eliminationCountWindowSize))

    // enrich the state with the new events
    val updatedState: FIFOBuffer[WeatherEvent] = values.foldLeft(currentState) {
      case (state, event) => state.add(event)
    }

    // update the state with the enriched state
    state.update(updatedState)
    state.setTimeoutDuration("30 seconds")

    // only when we have enough data, create a WeatherEventAverage from the accumulated state before that, we return an empty result.
    stateToAverageEvent(key, updatedState)
  }

  private def stateToAverageEvent(key: String, data: FIFOBuffer[WeatherEvent]): Iterator[WeatherEventAverage] = {
    if (data.size == eliminationCountWindowSize) {
      val events = data.get
      val start: WeatherEvent = events.head
      val end: WeatherEvent = events.last
      val pressureAvg = events.map(event => event.pressure).sum / data.size
      val tempAvg = events.map(event => event.temp).sum / data.size
      Iterator(WeatherEventAverage(key, start.timestamp, end.timestamp, pressureAvg, tempAvg))
    } else {
      Iterator.empty
    }
  }
}

// =================== Helpers

case class FIFOBuffer[T](capacity: Int, data: Queue[T] = Queue.empty) extends Serializable {
  def add(element: T): FIFOBuffer[T] =
    this.copy(data = data.enqueue(element).take(capacity))

  def get: List[T] = data.toList

  def size: Int = data.size
}

case class WeatherEventAverage(stationId: String,
                               startTime: Timestamp,
                               endTime:Timestamp,
                               pressureAvg: Double,
                               tempAvg: Double)

object WeatherEventAverage {
  def empty(key: String): WeatherEventAverage = WeatherEventAverage(key, new Timestamp(0), new Timestamp(0), 0.0, 0.0 )

}

// ============== Domain

case class Rate(timestamp: Timestamp, value: Long)
case class WeatherEvent(stationId: String,
                        timestamp: Timestamp,
                        location: (Double,Double),
                        pressure: Double,
                        temp: Double)

object WeatherEvent {

  def generate(ts: Timestamp): WeatherEvent =
    WeatherEvent(pickOne(uids), ts, locationGenerator(), pressureGen(), tempGen())

  val uids = List("d1e46a42", "d8e16e2a", "d1b06f88", "d2e710aa", "d2f731cc", "d4c162ee","d4a11632", "d7e277b2", "d59018de", "d60779f6")

  def pickOne[T](list: List[T]): T = list(Random.nextInt(list.size))

  val locationGenerator: () => (Double, Double) = {
    // Europe bounds
    val longBounds = (-10.89,39.82)
    val latBounds = (35.52,56.7)

    def pointInRange(bounds: (Double, Double)): Double = {
      val (a, b) = bounds
      Math.abs(Random.nextDouble()) * b + a
    }

    () => (pointInRange(longBounds), pointInRange(latBounds))
  }

  val pressureGen: () => Double = () => Random.nextDouble + 101.0
  val tempGen: () => Double = () => Random.nextDouble * 60 - 20
}