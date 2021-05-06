package org.esgi.project.api

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, VisitCountResponse, ViewCountResponse}
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
      //TEST PATH
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
      },
      path("movies" / Segment) { period: String =>
        get {
          //KV STORE TOTAL
          val kvStoreTotal: ReadOnlyKeyValueStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.totalViewsGroupedByTitleStoreName, QueryableStoreTypes.keyValueStore[String, Long]())
            )

          //KV STORE START ONLY
          val kvStoreTotalStartOnly: ReadOnlyKeyValueStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.totalViewsStartOnlyStoreName, QueryableStoreTypes.keyValueStore[String, Long]())
            )

          val kvStoreLastMinStartOnly: ReadOnlyWindowStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.lastMinViewsStartOnlyStoreName, QueryableStoreTypes.windowStore[String, Long]())
            )

          val kvStoreFiveLastMinStartOnly: ReadOnlyWindowStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.fiveLastMinViewsStartOnlyStoreName, QueryableStoreTypes.windowStore[String, Long]())
            )
          /////////////////////////////////////
          //KV STORE HALF
          val kvStoreTotalHalf: ReadOnlyKeyValueStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.totalViewsHalfStoreName, QueryableStoreTypes.keyValueStore[String, Long]())
            )

          val kvStoreLastMinHalf: ReadOnlyWindowStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.lastMinViewsHalfStoreName, QueryableStoreTypes.windowStore[String, Long]())
            )

          val kvStoreFiveLastMinHalf: ReadOnlyWindowStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.fiveLastMinViewsHalfStoreName, QueryableStoreTypes.windowStore[String, Long]())
            )
          ////////////////////////////////
          //KV STORE FULL
          val kvStoreTotalFull: ReadOnlyKeyValueStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.totalViewsFullStoreName, QueryableStoreTypes.keyValueStore[String, Long]())
            )

          val kvStoreLastMinFull: ReadOnlyWindowStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.lastMinViewsFullStoreName, QueryableStoreTypes.windowStore[String, Long]())
            )

          val kvStoreFiveLastMinFull: ReadOnlyWindowStore[String, Long] = streams
            .store(
              StoreQueryParameters
                .fromNameAndType(StreamProcessing.fiveLastMinViewsFullStoreName, QueryableStoreTypes.windowStore[String, Long]())
            )

          ////GET DATA////

          //GET KV STORES KEYS
          //get TOTALS
          val availableKeysKvstoreTotal = kvStoreTotal.all().asScala.map(_.key).toList.distinct
          val availableKeysKvStoreTotalStartOnly = kvStoreTotalStartOnly.all().asScala.map(_.key).toList.distinct
          val availableKeysKvStoreTotalHalf = kvStoreTotalHalf.all().asScala.map(_.key).toList.distinct
          val availableKeysKvStoreTotalFull = kvStoreTotalFull.all().asScala.map(_.key).toList.distinct

          //get LAST MIN
          val availableKeysKvStoreLastMinuteStartOnly = kvStoreLastMinStartOnly.all().asScala.map(_.key.key()).toList.distinct
          val availableKeysKvStoreLastMinuteHalf = kvStoreLastMinHalf.all().asScala.map(_.key.key()).toList.distinct
          val availableKeysKvStoreLastMinuteFull = kvStoreLastMinFull.all().asScala.map(_.key.key()).toList.distinct

          //get FIVE LAST MIN
          val availableKeysKvStoreFiveLastMinuteStartOnly = kvStoreFiveLastMinStartOnly.all().asScala.map(_.key.key()).toList.distinct
          val availableKeysKvStoreFiveLastMinuteHalf = kvStoreFiveLastMinHalf.all().asScala.map(_.key.key()).toList.distinct
          val availableKeysKvStoreFiveLastMinuteFull = kvStoreFiveLastMinFull.all().asScala.map(_.key.key()).toList.distinct

          //GET VALUES
          //get TOTALS

          val KvstoreTotalValues = availableKeysKvstoreTotal
            .map(storeKeyTotalToViewCount(kvStoreTotal))
            .sortBy(_.count)(implicitly[Ordering[Long]].reverse)


          val kvstoreTotalStartOnlyValues = availableKeysKvStoreTotalStartOnly
            .map(storeKeyTotalToViewCount(kvStoreTotalStartOnly))
            .sortBy(_.count)(implicitly[Ordering[Long]].reverse)


          val kvstoreTotalHalfValues = availableKeysKvStoreTotalHalf
            .map(storeKeyTotalToViewCount(kvStoreTotalHalf))
            .sortBy(_.count)(implicitly[Ordering[Long]].reverse)

          val kvstoreTotalFullValues = availableKeysKvStoreTotalFull
            .map(storeKeyTotalToViewCount(kvStoreTotalFull))
            .sortBy(_.count)(implicitly[Ordering[Long]].reverse)



          //get LAST MIN
          val toTime = Instant.now()
          val fromTime = toTime.minusSeconds(60)
          val kvstoreLastMinuteStartOnlyValues = availableKeysKvStoreLastMinuteStartOnly
            .map(storeKeyLastMinToViewCount(kvStoreLastMinStartOnly, fromTime, toTime))
            .sortBy(_.count)(implicitly[Ordering[Long]].reverse)


          //get total
          complete(
            kvstoreLastMinuteStartOnlyValues
            //kvstoreTotalStartOnlyValues
          )
        }
      }

    )
  }

  def storeKeyToViewCount(store: ReadOnlyWindowStore[String, Long], from: Instant, to: Instant)(key: String): VisitCountResponse = {
    val row: WindowStoreIterator[Long] = store.fetch(key, from, to)
    val last_value = row.asScala.toList.last.value
    System.out.println("KEY/VALUE FOR TEST WINDOW STORE "+key+"/"+last_value)
    VisitCountResponse(view_category = key, count = last_value)
  }

  //FUNCTIONS TO GET TOTAL
  def storeKeyTotalToViewCount(store: ReadOnlyKeyValueStore[String, Long])(key: String): ViewCountResponse = {
    val row:Long = store.get(key)
    //System.err.println("VALUE FOR COUNTERE "+row)
    ViewCountResponse(title = key, count = row)
  }

  //FUNCTIONS TO GET LAST MIN
  def storeKeyLastMinToViewCount(store: ReadOnlyWindowStore[String, Long], from: Instant, to: Instant)(key: String): ViewCountResponse = {
    val row: WindowStoreIterator[Long] = store.fetch(key, from, to)
    val last_value = row.asScala.toList.last.value
    System.out.println("KEY/VALUE FOR LAST MINS WINDOW STORE "+key+"/"+last_value)
    ViewCountResponse(title = key, count = last_value)
  }
}
