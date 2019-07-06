/*******************************************************************************
 *
 * Copyright (C) 2018 by Obsidian SAS : https://dataintoresults.com/
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

package com.dataintoresults.etl.datastore.hubspot

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Try, Failure, Success}

import scala.collection.JavaConverters._
import play.api.Logger
import com.typesafe.config.Config

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.json._
import play.api.libs.ws._
import play.api.libs.ws.ahc._

import com.dataintoresults.etl.core.{Column, DataSink, DataSource, DataStore, Table}
import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.DataSetImpl
import com.dataintoresults.util.InHelper._


class HubspotStore extends EtlDatastore with DataStore {
  private val logger: Logger = Logger(this.getClass())
  

  private val _tables = EtlChilds[HubspotTable]()
  private val _apiKey = EtlParameter[String](nodeAttribute = "apiKey", configAttribute = "dw.datastore."+name+".apiKey", defaultValue="")
  private var lastCallTimestamp = 0L
  private val delayBetweenCalls = 110L

  def apiKey = _apiKey.value()
  
  override def toString = s"HubspotStore[${name}]"

  override def addTable(spec : scala.xml.Node) : Unit = ???

  override def removeTable(tableName: String): Unit = ???

  def tables() : Seq[Table] = _tables



  def createDataSource(hTable: HubspotTable) : DataSource = {


    // We create a DataSource on top of the DataSet
    new DataSource {    
      import DefaultBodyReadables._
      import JsonBodyReadables._
      import scala.concurrent.ExecutionContext.Implicits._


      private val endPoint = hTable.endpoint
      private val root = hTable.root

      private var nbCalls = 0
      private var nbRows = 0

      private var hasMoreValue = true
      private var offsetValue = -1L
      private var offset2Value = -1L
      private var firstCall = true
      private var iterator: Iterator[Seq[Object]] = new Iterator[Seq[Object]]{
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
        if(!firstCall && !hasMoreValue) 
          return

        var url = endPoint+"?hapikey="+apiKey 

        // For subequent calls we need an offset to tell Hubspot from where to start
        if(!firstCall) {
          url = url + "&" + hTable.offsetUrl + "=" + offsetValue
          if(hTable.has2Offsets)
            url = url + "&" + hTable.offset2Url + "=" + offset2Value  
        }

        // If we have a specific maximum number of rows to retrieve        
        var maxRowsLog = ""
        if(hTable.rowsPerCallUrl != "") {
          url = url + "&" + hTable.rowsPerCallUrl + "=" + hTable.rowsPerCall  
          maxRowsLog = s" (max ${hTable.rowsPerCall} rows)"   
        }
        
        // If we have a url attributes to add    
        if(hTable.urlAttributes != "") {
          url = url + "&" + hTable.urlAttributes 
        }

        logger.info(s"Loading data$maxRowsLog from ${url.replaceFirst("=[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}&", "="+apiKey.take(2)+"????&")}")

        val json = retry(5) {
          // Avoid to call the API too often
          val currentTimestamp = System.currentTimeMillis() 
          if(currentTimestamp < lastCallTimestamp + delayBetweenCalls) {
            Thread.sleep(lastCallTimestamp + delayBetweenCalls - currentTimestamp)
          }
          
          nbCalls = nbCalls + 1
          val response = ws.url(url).get().map{
            resp => if(resp.status != 200) {
              throw new RuntimeException(s"Query returned statuts ${resp.status}")
            } else {
              resp.body[JsValue]
            }
          }

          lastCallTimestamp = System.currentTimeMillis() 

          Await.result(response, 10 second)
        } match {
          case Failure(e) => throw e
          case Success(s) => s
        }

        // Check if there is a batch after this one        
        hasMoreValue = 
          if(hTable.hasMore != "") {
            (json \ hTable.hasMore).asOpt[Boolean].getOrElse {
              val jsonStr = json.toString
              val jsonLog = jsonStr.take(30) + " (...) " + jsonStr.takeRight(30)
              val log = s"Couldn't find the ${hTable.hasMore} attribute in the resulting JSON $jsonLog."
              logger.error(log)
              throw new RuntimeException(log)
            }
          }
          else {
            false
          }
        offsetValue = (json \ hTable.offset).asOpt[Long].getOrElse(-1)
        val total = (json \ "total").asOpt[Long]

        // If offset2 attribute is set in the table definition
        if(hTable.has2Offsets)
          offset2Value = (json \ hTable.offset2).asOpt[Long].getOrElse(-1)


        val root = (json \ hTable.root).asOpt[List[JsObject]].getOrElse {
          val jsonStr = json.toString
          val jsonLog = jsonStr.take(30) + " (...) " + jsonStr.takeRight(30)
          val log = s"Couldn't find the root ${hTable.hasMore} attribute in the resulting JSON $jsonLog."
          logger.error(log)
          throw new RuntimeException(log)
        }
        // Convert the JSON respone to a dataset
        val dataset = root.map { jrow =>
          hTable.hubspotColumns.map[Object, Seq[Object]] { col =>
            val jsValue = col.path match {
              case "." => jrow
              case _ => col.path.split("\\.").foldLeft[JsValue](jrow)( (json, sel) => {
                (json \ sel).getOrElse(JsNull) 
              })
            }
            col.basicType match {
            case Column.INT => jsValue.asOpt[Int].getOrElse(null).asInstanceOf[AnyRef]             
            case Column.BIGINT => jsValue.asOpt[Long].getOrElse(null).asInstanceOf[AnyRef]   
            case Column.TEXT => jsValue match {
              case JsString(s) => s
              case o: JsValue => o.toString
              case _ => null
            }
            case Column.NUMERIC => jsValue.asOpt[Double].getOrElse(null).asInstanceOf[AnyRef]   
            case Column.BOOLEAN => jsValue.asOpt[Boolean].getOrElse(null).asInstanceOf[AnyRef]
            case _ => throw new RuntimeException(s"Hubspot connector doesn't support ${col.colType} type.")
            }
          }
        }

        // No longer the firt call
        firstCall = false

        // Log how much we advanced
        nbRows = nbRows + dataset.size
        val totalStr = total.map(t => s" (of ${total.get} to read)").getOrElse("")
        logger.info(s"$nbCalls API calls and $nbRows read so far$totalStr.")

        // Setting the new iterator to iterate on this batch
        iterator = dataset.iterator
      }



      def structure = hTable.columns.seq

      def hasNext() = {
        iterator.hasNext match {
          case true => true
          case false => hasMoreValue match {
            case true => {
              loadBatch()
              hasNext()
            }
            case false => false
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


  def close() : Unit = {
  }
}

object HubspotStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): DataStore = {
    val store = new HubspotStore()
    store.parse(dsXml, config)
    store
  }
}
