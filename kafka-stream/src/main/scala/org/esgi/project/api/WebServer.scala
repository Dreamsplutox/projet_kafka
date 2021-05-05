package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, VisitCountResponse}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.MeanLatencyForURL

import java.time.Instant
import scala.jdk.CollectionConverters._

/**
 * -------------------
 * Part.3 of exercise: Interactive Queries
 * -------------------
 */
object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("views" / Segment) { period: String =>
        get {
          period match {
            case "30s" =>
              // load our materialized store
              val kvStore30Seconds: ReadOnlyWindowStore[String, Long] = streams
                .store(
                  StoreQueryParameters
                    .fromNameAndType(StreamProcessing.thirtySecondsStoreName, QueryableStoreTypes.windowStore[String, Long]())
                )
              // fetch all available keys
              val availableKeys = kvStore30Seconds.all().asScala.map(_.key.key()).toList.distinct
              // define our time interval to fetch the last window (nearest one to now)
              val toTime = Instant.now()
              val fromTime = toTime.minusSeconds(30)

              complete(
                availableKeys
                  .map(storeKeyToViewCount(kvStore30Seconds, fromTime, toTime))
                  .sortBy(_.count)(implicitly[Ordering[Long]].reverse)
              )
            case "1m" =>
              // load our materialized store
              val kvStore1Minute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(StreamProcessing.lastMinuteStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              // fetch all available keys
              val availableKeys = kvStore1Minute.all().asScala.map(_.key.key()).toList.distinct
              // define our time interval to fetch the last window (nearest one to now)
              val toTime = Instant.now()
              val fromTime = toTime.minusSeconds(60)

              complete(
                availableKeys
                  .map(storeKeyToViewCount(kvStore1Minute, fromTime, toTime))
                  .sortBy(_.count)(implicitly[Ordering[Long]].reverse)
              )
            case "5m" =>
              // load our materialized store
              val kvStore5Minute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(StreamProcessing.lastFiveMinutesStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              // fetch all available keys
              val availableKeys = kvStore5Minute.all().asScala.map(_.key.key()).toList.distinct
              // define our time interval to fetch the last window (nearest one to now)
              val toTime = Instant.now()
              val fromTime = toTime.minusSeconds(5 * 60)

              complete(
                availableKeys
                  .map(storeKeyToViewCount(kvStore5Minute, fromTime, toTime))
                  .sortBy(_.count)(implicitly[Ordering[Long]].reverse)
              )
            case _ =>
              // unhandled period asked
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
              )
          }
        }
      }//,
      /*path("visits-per-category" / Segment) { period: String =>
        get {
          period match {
            case "30s" =>
              // load our materialized store
              val kvStore30Seconds: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(StreamProcessing.thirtySecondsByCategoryStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              // fetch all available keys
              val availableKeys = kvStore30Seconds.all().asScala.map(_.key.key()).toList.distinct
              // define our time interval to fetch the last window (nearest one to now)
              val toTime = Instant.now()
              val fromTime = toTime.minusSeconds(30)

              complete(
                availableKeys
                  .map(storeKeyToVisitCount(kvStore30Seconds, fromTime, toTime))
                  .sortBy(_.count)(implicitly[Ordering[Long]].reverse)
              )
            case "1m" =>
              // load our materialized store
              val kvStore1Minute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(StreamProcessing.lastMinuteByCategoryStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              // fetch all available keys
              val availableKeys = kvStore1Minute.all().asScala.map(_.key.key()).toList.distinct
              // define our time interval to fetch the last window (nearest one to now)
              val toTime = Instant.now()
              val fromTime = toTime.minusSeconds(60)

              complete(
                availableKeys
                  .map(storeKeyToVisitCount(kvStore1Minute, fromTime, toTime))
                  .sortBy(_.count)(implicitly[Ordering[Long]].reverse)
              )
            case "5m" =>
              // load our materialized store
              val kvStore5Minute: ReadOnlyWindowStore[String, Long] = streams
                .store(StoreQueryParameters.fromNameAndType(StreamProcessing.lastFiveMinutesByCategoryStoreName, QueryableStoreTypes.windowStore[String, Long]()))
              // fetch all available keys
              val availableKeys = kvStore5Minute.all().asScala.map(_.key.key()).toList.distinct
              // define our time interval to fetch the last window (nearest one to now)
              val toTime = Instant.now()
              val fromTime = toTime.minusSeconds(5 * 60)

              complete(
                availableKeys
                  .map(storeKeyToVisitCount(kvStore5Minute, fromTime, toTime))
                  .sortBy(_.count)(implicitly[Ordering[Long]].reverse)
              )
            case _ =>
              // unhandled period asked
              complete(
                HttpResponse(StatusCodes.NotFound, entity = "Not found")
              )
          }
        }
      },*/
      /*
      path("latency" / "beginning") {
        get {
          // load our materialized store
          val kvStoreMeanLatencyPerURL: ReadOnlyKeyValueStore[String, MeanLatencyForURL] = streams
            .store(StoreQueryParameters.fromNameAndType(StreamProcessing.meanLatencyForURLStoreName, QueryableStoreTypes.keyValueStore[String, MeanLatencyForURL]()))
          // fetch all available keys
          val availableKeys: List[String] = kvStoreMeanLatencyPerURL.all().asScala.map(_.key).toList

          complete(
            availableKeys
              .map(storeKeyToMeanLatencyForURL(kvStoreMeanLatencyPerURL))
              .sortBy(_.meanLatency)(implicitly[Ordering[Long]].reverse)
          )
        }
      }*/
    )
  }

  def storeKeyToViewCount(store: ReadOnlyWindowStore[String, Long], from: Instant, to: Instant)(key: String): VisitCountResponse = {
    val row: WindowStoreIterator[Long] = store.fetch(key, from, to)
    VisitCountResponse(view_category = key, count = row.asScala.toList.last.value)
  }

  /*def storeKeyToVisitCount(store: ReadOnlyWindowStore[String, Long], from: Instant, to: Instant)(key: String): VisitCountResponse = {
    val row: WindowStoreIterator[Long] = store.fetch(key, from, to)
    VisitCountResponse(url = key, count = row.asScala.toList.last.value)
  }

  def storeKeyToMeanLatencyForURL(store: ReadOnlyKeyValueStore[String, MeanLatencyForURL])(key: String): MeanLatencyForURLResponse = {
    val row: MeanLatencyForURL = store.get(key)
    MeanLatencyForURLResponse(url = key, meanLatency = row.meanLatency)
  }*/
}
