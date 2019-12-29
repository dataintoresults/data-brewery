/*******************************************************************************
 *
 * Copyright (C) 2019 by Obsidian SAS : https://dataintoresults.com/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package com.dataintoresults.etl.datastore.bitmovin


import scala.collection.immutable.HashSet
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}
import scala.collection.JavaConverters._

import java.io.File
import java.io.IOException
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDate, ZoneOffset, Instant}
import java.util.Arrays

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import play.api.Logger
import play.api.libs.json._
import play.api.libs.ws._
import play.api.libs.ws.ahc._
import play.api.libs.ws.JsonBodyWritables._
import play.api.Logger
import play.api.libs.json.{ JsNull, Json, JsString, JsValue }

import com.typesafe.config.Config
import com.dataintoresults.etl.core.{Column, DataSink, DataSource, DataStore, Table}
import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.DataSetImpl
import com.dataintoresults.util.InHelper._
import java.time.LocalDateTime
import com.dataintoresults.etl.impl._


sealed class Item(val column: String, val col: BitmovinColumn)

case class GroupBy(_column: String, _col: BitmovinColumn) extends Item(_column, _col)
case class Interval(_column: String, _col: BitmovinColumn) extends Item(_column, _col)
class Aggregation(_column: String, _col: BitmovinColumn) extends Item(_column, _col)
case class Count(_column: String, _col: BitmovinColumn) extends Aggregation(_column, _col)
case class Sum(_column: String, _col: BitmovinColumn) extends Aggregation(_column, _col)
case class Min(_column: String, _col: BitmovinColumn) extends Aggregation(_column, _col)
case class Max(_column: String, _col: BitmovinColumn) extends Aggregation(_column, _col)
case class Avg(_column: String, _col: BitmovinColumn) extends Aggregation(_column, _col)
case class StdDev(_column: String, _col: BitmovinColumn) extends Aggregation(_column, _col)
case class Percentile(_column: String, _col: BitmovinColumn, percentile: Int) extends Aggregation(_column, _col)
case class Variance(_column: String, _col: BitmovinColumn) extends Aggregation(_column, _col)
case class Median(_column: String, _col: BitmovinColumn) extends Aggregation(_column, _col)

class BitmovinStore extends EtlDatastore with DataStore {
  private val logger: Logger = Logger(this.getClass())

  private val _tables = EtlChilds[BitmovinTable]()

	private val _apiKey = EtlParameter[String](nodeAttribute="apiKey", configAttribute="dw.datastore."+name+".apiKey")
	private val _licenseKey = EtlParameter[String](nodeAttribute="licenseKey", configAttribute="dw.datastore."+name+".licenseKey")
	private val _organizationId = EtlParameter[String](nodeAttribute="organizationId", configAttribute="dw.datastore."+name+".organizationId", defaultValue = "")

	private val _limitPerBatch = EtlParameter[Int](nodeAttribute="limitPerBatch", configAttribute="dw.datastore."+name+".limitPerBatch", defaultValue = 200)
	private val _waitPerCall = EtlParameter[Int](nodeAttribute="waitPerCall", configAttribute="dw.datastore."+name+".waitPerCall", defaultValue = 10)
	private val _retryPerCall = EtlParameter[Int](nodeAttribute="retryPerCall", configAttribute="dw.datastore."+name+".retryPerCall", defaultValue = 5)
	private val _delayBetweenCalls = EtlParameter[Int](nodeAttribute="delayBetweenCalls", configAttribute="dw.datastore."+name+".delayBetweenCalls", defaultValue = 110)

  private final val baseEndpoint = "https://api.bitmovin.com/v1"
  private final val countEndpoint = baseEndpoint + "/analytics/queries/count"
  private final val sumEndpoint = baseEndpoint + "/analytics/queries/sum"
  private final val minEndpoint = baseEndpoint + "/analytics/queries/min"
  private final val maxEndpoint = baseEndpoint + "/analytics/queries/max"
  private final val avgEndpoint = baseEndpoint + "/analytics/queries/avg"
  private final val stddevEndpoint = baseEndpoint + "/analytics/queries/stddev"
  private final val percentileEndpoint = baseEndpoint + "/analytics/queries/percentile"
  private final val varianceEndpoint = baseEndpoint + "/analytics/queries/variance"
  private final val medianEndpoint = baseEndpoint + "/analytics/queries/median"

	def apiKey = _apiKey.value
  def licenseKey = _licenseKey.value
  def organizationId = _organizationId.value
  
  private var lastCallTimestamp = 0L
  private def delayBetweenCalls = _delayBetweenCalls.value()
  private def limitPerBatch = _limitPerBatch.value()
  private def waitPerCall = _waitPerCall.value()
  private def retryPerCall = _retryPerCall.value()

  /*
   * List of columns on the Bitmovin side
   */
  private val bitmovinIntervals = HashSet("MINUTE", "HOUR", "DAY", "MONTH")

  private val bitmovinColumns = bitmovinIntervals ++ HashSet(
    "IMPRESSION_ID", "ACTIVE_PLAYER_STARTUPTIME", "AD", "ANALYTICS_VERSION", "ASN", "AUDIO_BITRATE",
    "AUDIO_CODEC", "AUTOPLAY", "BROWSER", "BROWSER_VERSION_MAJOR", "BROWSER_IS_BOT", "BUFFERED", "CDN_PROVIDER", "CITY", 
    "CLIENT_TIME", "COUNTRY", "CUSTOM_DATA_1", "CUSTOM_DATA_2", "CUSTOM_DATA_3", "CUSTOM_DATA_4", "CUSTOM_DATA_5", "CUSTOM_USER_ID", 
    "DAY", "DEVICE_CLASS", "DEVICE_TYPE", "DOMAIN", "DRM_LOAD_TIME", "DRM_TYPE", "DROPPED_FRAMES", "DURATION", "ERROR_CODE", 
    "ERROR_MESSAGE", "ERROR_RATE", "EXPERIMENT_NAME", "HOUR", "INITIAL_TIME_TO_TARGET_LATENCY", "IP_ADDRESS", "IS_CASTING", 
    "IS_LIVE", "IS_LOW_LATENCY", "IS_MUTED", "ISP", "LANGUAGE", "LATENCY", "LICENSE_KEY", "M3U8_URL", "MINUTE", "MONTH", "MPD_URL", 
    "OPERATINGSYSTEM", "OPERATINGSYSTEM_VERSION_MAJOR", "PAGE_LOAD_TIME", "PAGE_LOAD_TYPE", "PATH", "PAUSED", "PLATFORM", "PLAYED", 
    "PLAYER", "PLAYER_KEY", "PLAYER_STARTUPTIME", "PLAYER_TECH", "PLAYER_VERSION", "PROG_URL", "REGION", "SCALE_FACTOR", 
    "SCREEN_HEIGHT", "SCREEN_WIDTH", "SEEKED", "SEQUENCE_NUMBER", "SIZE", "STARTUPTIME", "STREAM_FORMAT", "SUPPORTED_VIDEO_CODECS", 
    "TARGET_LATENCY", "TARGET_LATENCY_DELTA", "TIME", "TIME_TO_TARGET_LATENCY", "USER_ID", "VIDEO_BITRATE", "VIDEO_CODEC", 
    "VIDEO_DURATION", "VIDEO_ID", "VIDEO_PLAYBACK_HEIGHT", "VIDEO_PLAYBACK_WIDTH", "VIDEO_STARTUPTIME", "VIDEO_TITLE", 
    "VIDEO_WINDOW_HEIGHT", "VIDEO_WINDOW_WIDTH", "VIDEOTIME_END", "VIDEOTIME_START")

  override def toString = s"BitmovinStore[${name}]"

  def tables() : Seq[Table] = _tables
  

  def createDataSource(table: BitmovinTable) : DataSource = {
    val items : Seq[Item] = parseColumns(table)
    val dimensions = items.filter(_.isInstanceOf[GroupBy]).map(_.asInstanceOf[GroupBy])
    
    val aggregs = items.filter(_.isInstanceOf[Aggregation]).map(_.asInstanceOf[Aggregation])
    // Currently we can process only one aggregation per table
    if(aggregs.size != 1) {
      throw new RuntimeException(s"There should be one and only one aggregation (sum or count) in table ${this.name}.${table.name}. There is ${aggregs.size} aggregation ${aggregs.map(_.column).mkString("(",",",")")}.")
    }
    val aggreg = aggregs.head

    val intervals = items.filter(_.isInstanceOf[Interval]).map(_.asInstanceOf[Interval])
    // We need one, and only one, interval per query
    if(intervals.size > 1) {
      throw new RuntimeException(s"There should be one and only one interval ${bitmovinIntervals.mkString("(",",",")")} in table ${this.name}.${table.name}. There is ${intervals.size} ${intervals.map(_.column).mkString("(",",",")")}.")
    }


    val queryPercentile = aggreg match {
      case Percentile(_, _, percentile) => Json.obj("percentile" -> percentile)
      case _ => Json.obj()
    }
    

    val queryGroupBy = 
      if(dimensions.size > 0)
        Json.obj("groupBy" -> Json.toJson(dimensions.map(_.column)))
      else 
        Json.obj()

    val queryFilters = 
      if(table.filters.size > 0)
        Json.obj("filters" -> { table.filters.map(_.filter).map { filter: FilterOp => Json.obj(
          "name" -> filter.left.value.toUpperCase(), 
          "operator" -> {filter match {
            case _: FilterEq => "EQ"
            case _: FilterNeq => "NE"
            case _: FilterSup => "GT"
            case _: FilterInf => "LT"
            case _: FilterSupEq => "GTE"
            case _: FilterInfEq => "LT"
          }},
          "value" -> filter.right.toJson
          )
        }})
      else 
        Json.obj()

    val queryInterval = intervals.map{ i => Json.obj("interval" -> i._column)}.headOption.getOrElse(Json.obj())

    var queryBase = Json.obj(
      "start" -> parseDate(table.startDate),
      "end" -> parseDate(table.endDate),
      "licenseKey" -> this.licenseKey,
      "dimension" -> aggreg.column,
      "limit" -> limitPerBatch
    ) ++ queryPercentile ++ queryInterval ++ queryGroupBy ++ queryFilters

    logger.debug(s"Base query for Bitmovin table ${this.name}.${table.name}: ${queryBase}")

    var headers = Seq(("Accept","application/json"), ("Content-Type","application/json"), ("X-Api-Key", apiKey)) 
    
    if(organizationId != "")
      headers = headers ++ Seq(("X-Tenant-Org-Id", organizationId))

    logger.debug("Bitmovin header : " + headers.mkString("(", ", ", ")"))
    logger.debug("Bitmovin query : " + queryBase.toString())

    val queryItems = intervals ++ dimensions ++ aggregs
    val queryMapping = createMapping(queryItems.map(_.col.formula).toArray, table.columns.map(_.formula).toArray)
    val queryEndpoint = aggreg match {
      case _: Count => countEndpoint
      case _: Sum => sumEndpoint
      case _: Min => minEndpoint
      case _: Max => maxEndpoint
      case _: Avg => avgEndpoint
      case _: StdDev => stddevEndpoint
      case _: Percentile => percentileEndpoint
      case _: Variance => varianceEndpoint
      case _: Median => medianEndpoint
    }
  
    new DataSource {
      import DefaultBodyReadables._
      import JsonBodyReadables._
      import scala.concurrent.ExecutionContext.Implicits._
    
    
      private val items = queryItems
      private val columns = table.columns
      private val endPoint = queryEndpoint
      private val mapping = queryMapping
    
      private var hasMoreValue = true
      private var nbCalls = 0
      private var nbRows = 0L

      private var iterator: Iterator[IndexedSeq[Object]] = new Iterator[IndexedSeq[Object]]{
        def hasNext() = false
        def next() = ???
      }
    
      private implicit val system = ActorSystem() 
      private implicit val materializer = ActorMaterializer()
      private val ws = StandaloneAhcWSClient()
    
    
      // Returning a Try[T] wrapper
      @annotation.tailrec
      def retry[T](n: Int)(fn: => T): Try[T] = {
        Try { fn } match {
          case Failure(e) if n > 1 => {
            logger.warn(s"Error during the call ${e}. Let's retry (${n-1} attempts left).")
            // Wait a second
            Thread.sleep(1000L)
            retry(n - 1)(fn)
          }
          case s => s
        }
      }
    
      private def loadBatch(): Unit = {
        // If not the first call and no next, there is nothing to do
        if(!hasMoreValue) {
          return
        }
        

        val query = queryBase + ("offset", Json.toJson(nbRows))


        val response = retry(retryPerCall) {
          // Avoid to call the API too often
          val currentTimestamp = System.currentTimeMillis() 
          if(currentTimestamp < lastCallTimestamp + delayBetweenCalls) {
            Thread.sleep(lastCallTimestamp + delayBetweenCalls - currentTimestamp)
          }
        

          nbCalls = nbCalls + 1
          val response = ws.url(endPoint)
            .addHttpHeaders(headers:_*)
            .post(query) 
            .map {
              resp => if(resp.status != 200) {
                throw new RuntimeException(s"Query returned statuts ${resp.status}. ${resp.body}")
              } else {
                resp.body[JsValue]
              }
            }
          lastCallTimestamp = System.currentTimeMillis() 
    
          Await.result(response, waitPerCall second)
        } match {
          case Failure(e) => throw e
          case Success(s) => s
        }
    
        

        val rowsJson = (response \ "data" \ "result" \ "rows").asOpt[JsArray]

        iterator = rowsJson.map { 
          case rows : JsArray => rows.value.map({ 
            case row : JsArray => parseRow(row.value)
            case _ => throw new RuntimeException(s"Unexpected empty row from ${name}.${table.name}.")
          })
          case _ => throw new RuntimeException(s"Unexpected absence of data from ${name}.${table.name}.")
        }.get.iterator

        val nbRowsRead = (response \"data" \ "result" \ "rowCount").as[Int]

        nbRows += nbRowsRead

        if(nbRowsRead < 200)
          hasMoreValue = false
      } 
    
      private def parseRow(values: IndexedSeq[JsValue]) : IndexedSeq[AnyRef] = {
        val convertedValues = values.zip(items.map(_.col)) map { case (jsValue: JsValue, col: BitmovinColumn) =>
          col.basicType match {
            case Column.INT => jsValue match {
              case JsNull => null 
              case JsString(s) => Try(s.toInt)
                .getOrElse(throw new RuntimeException(s"Bitmovin connector can't understand string ${jsValue.toString} as int in ${table.store.name}.${table.name}.${col.name}."))
                .asInstanceOf[AnyRef]
              case JsNumber(n) => jsValue.asOpt[Int].getOrElse(null).asInstanceOf[AnyRef]
              case _ => throw new RuntimeException(s"Bitmovin connector can't understand ${jsValue.toString} as int in ${table.store.name}.${table.name}.${col.name}.")
            }
            case Column.BIGINT => jsValue match {
              case JsNull => null 
              case JsString(s) => Try(s.toLong)
                .getOrElse(throw new RuntimeException(s"Bitmovin connector can't understand string ${jsValue.toString} as bigint in ${table.store.name}.${table.name}.${col.name}."))
                .asInstanceOf[AnyRef]
              case JsNumber(n) => jsValue.asOpt[Long].getOrElse(null).asInstanceOf[AnyRef]
              case _ => throw new RuntimeException(s"Bitmovin connector can't understand ${jsValue.toString} as bigint in ${table.store.name}.${table.name}.${col.name}.")
            }
            case Column.TEXT => jsValue match {
              case JsNull => null 
              case JsString(s) => s
              case o: JsValue => o.toString
              case _ => null
            }
            case Column.NUMERIC => jsValue.asOpt[Double].getOrElse(null).asInstanceOf[AnyRef]   
            case Column.BOOLEAN => jsValue.asOpt[Boolean].getOrElse(null).asInstanceOf[AnyRef]
            case Column.DATE => jsValue.asOpt[Long]
              .map(ts => Instant.ofEpochMilli(ts).atOffset(ZoneOffset.UTC).toLocalDate())
              .getOrElse(null).asInstanceOf[AnyRef]
            case Column.DATETIME => jsValue.asOpt[Long]
              .map(ts => Instant.ofEpochMilli(ts).atOffset(ZoneOffset.UTC).toLocalDateTime())
              .getOrElse(null).asInstanceOf[AnyRef]
            case _ => throw new RuntimeException(s"Bitmovin connector doesn't support ${col.colType} type for ${table.store.name}.${table.name}.")
          }
        }

        val result = Array.ofDim[AnyRef](columns.size)
        convertedValues.zip(mapping) foreach { case (v, idx) =>
          if(idx >= 0) {
            result(idx) = v
          }
        }

        result
      }
    
      def structure: Seq[BitmovinColumn] = columns
    
      def hasNext() = {
        iterator.hasNext match {
          case true => true
          case false => hasMoreValue match {
            case true => {
               loadBatch()
               hasNext()
            }
            case false => {
              close()
              false
            }
          }
        }
      }
    
      def next() = iterator.next()
    
      def close() = { 
        ws.close
        system.terminate
      }
    }
  }

  private def createMapping(s1: Array[String], s2: Array[String]) : Array[Int] = {
    val res = Array.fill[Int](s1.size)(-1)
    for(i <- 0 until s1.size) {
      for(j <- 0 until s2.size) {
        if(s1(i) == s2(j)) {
          res(i) = j
        }
      }
    }
    res
  }

  def close() : Unit = {
    // Nothing to do
  }

  private lazy val countRegex = "COUNT\\((.*)\\)".r
  private lazy val sumRegex = "SUM\\((.*)\\)".r
  private lazy val minRegex = "MIN\\((.*)\\)".r
  private lazy val maxRegex = "MAX\\((.*)\\)".r
  private lazy val avgRegex = "AVG\\((.*)\\)".r
  private lazy val stddevRegex = "STDDEV\\((.*)\\)".r
  private lazy val percentileRegex = """PERCENTILE\((.*)\s*,\s*(\d+)\)""".r
  private lazy val varianceRegex = "VARIANCE\\((.*)\\)".r
  private lazy val medianRegex = "MEDIAN\\((.*)\\)".r

    
  private def parseColumns(table: BitmovinTable): Seq[Item] = {
    table.columns map { col => 
      val formula = col.formula.toUpperCase()

      formula match {
        case f if bitmovinIntervals.contains(f) => Interval(formula, col)
        case f if bitmovinColumns.contains(f) => GroupBy(formula, col)
        case countRegex(c) => Count(c, col)
        case sumRegex(c) => Sum(c, col)
        case minRegex(c) => Min(c, col)
        case maxRegex(c) => Max(c, col)
        case avgRegex(c) => Avg(c, col)
        case stddevRegex(c) => StdDev(c, col)
        case percentileRegex(c, p) => Percentile(c, col, p.toInt)
        case varianceRegex(c) => Variance(c, col)
        case medianRegex(c) => Median(c, col)
        case _ => throw new RuntimeException(s"The Bitmovin column $formula is not recognized in column ${name}.${table.name}.${col.name}")
      }
    }
  }

  
  private lazy val dateRegex = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
  private lazy val agoRegex = """(\d+)day.?Ago""".r
  private def parseDate(date: String): String = {
    date match {
      case dateRegex(y,m,d) => LocalDateTime.of(y.toInt, m.toInt, d.toInt, 0, 0, 0).toInstant(ZoneOffset.UTC).toString()
      case agoRegex(ago) =>
      LocalDateTime.now(ZoneOffset.UTC)
          .minusDays(ago.toInt)
          .toInstant(ZoneOffset.UTC).toString()
    }
  }
}


object BitmovinStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): DataStore = {
    val store = new BitmovinStore()
    store.parse(dsXml, config)
    store
  }
}
