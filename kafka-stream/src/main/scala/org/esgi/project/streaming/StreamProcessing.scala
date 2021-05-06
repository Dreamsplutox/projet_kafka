package org.esgi.project.streaming

import io.github.azhur.kafkaserdeplayjson.PlayJsonSupport
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.esgi.project.streaming.models.{Like, MeanLatencyForURL, MeanScoreForTitle, View, ViewWithScore}

import java.io.InputStream
import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  //val yourFirstName: String = "Arnaud"
  //val yourLastName: String = "Simon"

  val applicationName = "movies-streaming-group5-arnaud"
  val viewsTopicName: String = "views"
  val likesTopicName: String = "likes"

  //Test datastore
  val thirtySecondsStoreName: String = "VisitsOfLast30Seconds"
  val lastMinuteStoreName = "VisitsOfLastMinute"
  val lastFiveMinutesStoreName = "VisitsOfLast5Minutes"

  //Part 1 store names
  val totalViewsGroupedByTitleStoreName = "TotalViewsByTitle"

  val totalViewsStartOnlyStoreName = "TotalViewsStartOnly"
  val totalViewsHalfStoreName = "TotalViewsHalfMovie"
  val totalViewsFullStoreName = "TotalViewsFullMovie"

  val lastMinViewsStartOnlyStoreName = "LastMinViewsStartOnly"
  val lastMinViewsHalfStoreName = "LastMinViewsHalfMovie"
  val lastMinViewsFullStoreName = "LastMinViewsFullMovie"

  val fiveLastMinViewsStartOnlyStoreName = "FiveLastMinViewsStartOnly"
  val fiveLastMinViewsHalfStoreName = "FiveLastMinViewsHalfMovie"
  val fiveLastMinViewsFullStoreName = "FiveLastMinViewsFullMovie"

  //TOP AND WORST Store names
  val meanScoreForTitleStoreName = "MeanScoreForTitle"

  val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // topic sources
  val views: KStream[String, View] = builder.stream[String, View](viewsTopicName)
  val likes: KStream[String, Like] = builder.stream[String, Like](likesTopicName)

  /**
   * -------------------
   * Part.0 of exercise ==> Basic test
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
   * Part.1 Creation of GET /movies/:id
   * -------------------
   */
  //CREATE USEFULL KTABLES
  val totalViewsByTitle: KTable[String, Long] = views.map((_, view) => (view.title, view))
    .groupByKey
    .count()(Materialized.as(totalViewsGroupedByTitleStoreName))

  //START ONLY SECTION
  val ViewsStartOnly : KGroupedStream[String, View] = views.filter((_,value)=>value.view_category.equals("start_only"))
    .map((_, view) => (view.title, view))
    .groupByKey

  val totalViewsStartOnly : KTable[String, Long] = ViewsStartOnly
    .count()(Materialized.as(totalViewsStartOnlyStoreName))


  val lastMinViewsStartOnly: KTable[Windowed[String], Long] = ViewsStartOnly
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(1)))
    //.windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(1)))
    .count()(Materialized.as(lastMinViewsStartOnlyStoreName))

  val lastFiveMinViewsStartOnly: KTable[Windowed[String], Long] = ViewsStartOnly
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(5)))
    .count()(Materialized.as(fiveLastMinViewsStartOnlyStoreName))


  //HALF SECTION
  val ViewsHalf : KGroupedStream[String, View] = views.filter((_,value)=>value.view_category.equals("half"))
    .map((_, view) => (view.title, view))
    .groupByKey

  val totalViewsHalf : KTable[String, Long] =  ViewsHalf.count()(Materialized.as(totalViewsHalfStoreName))

  val lastMinViewsHalf: KTable[Windowed[String], Long] = ViewsHalf
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(1)))
    .count()(Materialized.as(lastMinViewsHalfStoreName))

  val fiveLastMinViewsHalf: KTable[Windowed[String], Long] = ViewsHalf
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(5)))
    .count()(Materialized.as(fiveLastMinViewsHalfStoreName))

  //FULL SECTION
  val ViewsFull : KGroupedStream[String, View] = views.filter((_,value)=>value.view_category.equals("full"))
    .map((_, view) => (view.title, view))
    .groupByKey

  val totalViewsFull : KTable[String, Long] = ViewsFull.count()(Materialized.as(totalViewsFullStoreName))

  val lastMinViewsFull: KTable[Windowed[String], Long] = ViewsFull
    .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(1)))
    .count()(Materialized.as(lastMinViewsFullStoreName))

  val fiveLastMinViewsFull: KTable[Windowed[String], Long] = ViewsFull
    .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofSeconds(5)))
    .count()(Materialized.as(fiveLastMinViewsFullStoreName))

  /**
   * -------------------
   * Part.2 Creation of TOP AND WORST
   * -------------------
   */
  //TOP AND WORST SCORES
  val viewWithScore: KStream[String, ViewWithScore] = views
    .join(likes)({ (view, like) =>
      ViewWithScore(view._id, view.title, view.view_category, like.score)
    }, JoinWindows.of(Duration.ofSeconds(5)))

  val meanScorePerTitle: KTable[String, MeanScoreForTitle] = viewWithScore
    .map((_, viewWithScore) => (viewWithScore.title, viewWithScore))
    .groupByKey
    .aggregate(MeanScoreForTitle.empty) { (_, newViewWithScore, accumulator) =>
      accumulator.increment(score = newViewWithScore.score).computeMeanLatency
    }(Materialized.as(meanScoreForTitleStoreName))

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
