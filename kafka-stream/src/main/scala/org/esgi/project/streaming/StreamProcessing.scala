package org.esgi.project.streaming

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{MeanLatencyForURL, View, Like, VisitWithLatency}

import java.io.InputStream
import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  //val yourFirstName: String = "Arnaud"
  //val yourLastName: String = "Simon"

  val applicationName = "movies-streaming-group5"
  val viewsTopicName: String = "views"
  val likesTopicName: String = "likes"

  val thirtySecondsStoreName: String = "VisitsOfLast30Seconds"
  val lastMinuteStoreName = "VisitsOfLastMinute"
  val lastFiveMinutesStoreName = "VisitsOfLast5Minutes"

  val thirtySecondsByCategoryStoreName: String = "VisitsOfLast30SecondsByCategory"
  val lastMinuteByCategoryStoreName = "VisitsOfLastMinuteByCategory"
  val lastFiveMinutesByCategoryStoreName = "VisitsOfLast5MinutesByCategory"
  //val meanLatencyForURLStoreName = "MeanLatencyForURL"

  val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // topic sources
  val views: KStream[String, View] = builder.stream[String, View](viewsTopicName)
  val likes: KStream[String, Like] = builder.stream[String, Like](likesTopicName)

  /**
   * -------------------
   * Part.1 of exercise
   * -------------------
   */
  // Repartitioning views with view_category as key, then group them by key
  val viewsGroupedByCategory: KGroupedStream[String, View] = views
    .map((_, view) => (view.view_category, view))
    .groupByKey

  val visitsOfLast30Seconds: KTable[Windowed[String], Long] = viewsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofSeconds(30)).advanceBy(Duration.ofSeconds(1)))
    .count()(Materialized.as(thirtySecondsStoreName))

  val visitsOfLast1Minute: KTable[Windowed[String], Long] = viewsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(lastMinuteStoreName))

  val visitsOfLast5Minute: KTable[Windowed[String], Long] = viewsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(lastFiveMinutesStoreName))

  /**
   * -------------------
   * Part.2 of exercise
   * -------------------
   */
  /*
  val visitsGroupedByCategory: KGroupedStream[String, Visit] = visits
    .map((_, visit) => (visit.url.split("/")(1), visit))
    .groupByKey(Grouped.`with`)

  val visitsOfLast30SecondsByCategory: KTable[Windowed[String], Long] = visitsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofSeconds(30)).advanceBy(Duration.ofSeconds(1)))
    .count()(Materialized.as(thirtySecondsByCategoryStoreName))

  val visitsOfLast1MinuteByCategory: KTable[Windowed[String], Long] = visitsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(lastMinuteByCategoryStoreName))

  val visitsOfLast5MinuteByCategory: KTable[Windowed[String], Long] = visitsGroupedByCategory
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
    .count()(Materialized.as(lastFiveMinutesByCategoryStoreName))

  val visitsWithMetrics: KStream[String, VisitWithLatency] = visits
    .join(metrics)({ (visit, metric) =>
      VisitWithLatency(_id = visit._id, timestamp = visit.timestamp, sourceIp = visit.sourceIp, url = visit.url, latency = metric.latency)
    }, JoinWindows.of(Duration.ofSeconds(5)))

  val meanLatencyPerUrl: KTable[String, MeanLatencyForURL] = visitsWithMetrics
    .map((_, visitWithLatency) => (visitWithLatency.url, visitWithLatency))
    .groupByKey
    .aggregate(MeanLatencyForURL.empty) { (_, newVisitWithLatency, accumulator) =>
      accumulator.increment(latency = newVisitWithLatency.latency).computeMeanLatency
    }(Materialized.as(meanLatencyForURLStoreName))

  */

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run() {
        streams.close()
      }
    }))
    streams
  }

  // auto loader from properties file in project
  def buildProperties: Properties = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    import org.apache.kafka.streams.StreamsConfig
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("kafka.properties")

    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    // Disable caching to print the aggregation value after each record
    properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.load(inputStream)
    properties
  }
}
