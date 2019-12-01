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

package com.dataintoresults.etl.datastore.http

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

import java.io.File
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat

import akka.stream.ActorMaterializer
import akka.actor.ActorSystem

import play.api.libs.ws.ahc._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.DataSet
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.impl.DataSetImpl
import com.dataintoresults.etl.util.EtlHelper

import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

import com.gargoylesoftware.htmlunit.{WebClient, BrowserVersion}

class HttpStore extends EtlDatastore with DataStore {

	private val _tables = EtlChilds[HttpTable]()
  def tables: Seq[com.dataintoresults.etl.core.Table] = _tables

	override def toString = s"HttpStore[${_name}]" 

  def close(): Unit = {}
	

  
	def createDataSource(httpTable: HttpTable) : DataSource = {	  
		val datasetBuilder = Vector.newBuilder[Seq[Any]]

    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val ws = StandaloneAhcWSClient()
		
		val response = ws.url(httpTable.location).get().map{
      resp => resp.body
    }

		val plainResult = Await.result(response, 60 second)
				
		val dataset = httpTable.format match {
		  case "text" => dataSetText(httpTable.httpColumns, plainResult)
		  case "csv" => dataSetCsv(httpTable.httpColumns, plainResult, 
		      httpTable.csvSeparator, httpTable.csvQuote, httpTable.csvSkipRows, this.name+"."+httpTable.name)
		  case "html" => dataSetHtml(httpTable.httpColumns, plainResult, this.name+"."+httpTable.name)
		  case _ => throw new RuntimeException(s"Format ${httpTable.format} is not understood (should be either text, csv or html) for table ${name}.${httpTable.name}.")
		}

		ws.close()
		
    system.terminate()
		
		// We create a DataSource on top of the DataSet
		new DataSource {
		  private val iterator = dataset.rows.iterator
		  
		  def structure = dataset.columns
		  
		  def hasNext() = iterator.hasNext()
		  
		  def next() = iterator.next()
		  
		  def close() = {}
		}
	  
	}

	override def test() : Boolean = {
			true
	}
	
	private def dataSetText(httpColumns: Seq[HttpColumn], body: String) : DataSet = {
		  new DataSetImpl(httpColumns, Seq(Seq(body)))
	}
	
	private def dataSetCsv(httpColumns: Seq[HttpColumn], body: String, csvSeparator: String, csvQuote: String, csvSkipRows: Int, what: String) : DataSet = {
	  val lines = body.split("\n").slice(csvSkipRows, Int.MaxValue)
	  
	  // For instance if the quote is ", the string should be "\"" and not """
	  import org.apache.commons.lang3.StringEscapeUtils.escapeJava
    val quote = escapeJava(csvQuote)
    val separator = escapeJava(csvSeparator)
    
	  val data = lines map { line =>
	    //
	    val cells = line.split(s"$separator(?=([^$quote]*$quote[^$quote]*$quote)*[^$quote]*$$)", -1)
	    // We iterate over 
	    httpColumns.zipAll(cells, null, null) map { case (col, c) =>
	      val cell = c.replaceAll(s"^$quote|$quote$$", "");
	      htmlCellParse(col, cell)
	    }
	    
	  }
	  	  
	  new DataSetImpl(httpColumns, data)
	}
	
	
	private def dataSetHtml(httpColumns: Seq[HttpColumn], body: String, what: String) : DataSet = {
	  val jDoc = Jsoup.parse(body)


	  val parsers : Seq[Seq[Any]] = httpColumns map { col => jDoc.select(col.path) map { c => htmlCellParse(col, c.text) } }
	  
	  val maxLength = parsers map (_.length) max
			  
	  val parsers2 = parsers map (_.padTo(maxLength, null))
	  
	  val transposed = try {
	    parsers2.transpose
	  } catch {
	    case e: Exception => throw new RuntimeException(s"While parsing ${what}, there wasn't the same number of data point per columns (${ parsers map (_.length) mkString ","}). It should be the same.") 
	  }
	  
	  new DataSetImpl(httpColumns, parsers2.transpose)
	}
	
	private def htmlCellParse(column: HttpColumn, cell : String) : Any = {
	  if(cell == "") {
	    null
	  }
	  else {
			column.fromString(cell)
	  }
	}
	
}


object HttpStore {
	def fromXml(dsXml : scala.xml.Node, config: com.typesafe.config.Config) : DataStore = {
		val store = new HttpStore()
	  store.parse(dsXml, config)
		store
	}
}
