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

package com.dataintoresults.etl.datastore.googleSheets

import java.io.File
import java.util.Arrays
import scala.collection.JavaConversions._

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.analyticsreporting.v4.AnalyticsReportingScopes
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting
import com.google.api.services.analyticsreporting.v4.model.ColumnHeader
import com.google.api.services.analyticsreporting.v4.model.DateRange
import com.google.api.services.analyticsreporting.v4.model.Dimension
import com.google.api.services.analyticsreporting.v4.model.GetReportsRequest
import com.google.api.services.analyticsreporting.v4.model.GetReportsResponse
import com.google.api.services.analyticsreporting.v4.model.Metric
import com.google.api.services.analyticsreporting.v4.model.ReportRequest

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.Source
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.impl.source.EtlSource
import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core.{EtlTable, EtlParent, EtlChilds, EtlOptionalChild, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._


class GoogleSheetsTable extends EtlTable with Table {

  private final val defaultRowStart = "2" // start at 2 to take in account an header
  private final val defaultColStart = "A" // start at A
  private final val defaultSheet = "" // landing sheet

  private val _parent = EtlParent[GoogleSheetsStore]()

  private val _spreadsheetId = EtlParameter[String](nodeAttribute = "spreadsheetId", configAttribute = "dw.datastore."+name+".spreadsheetId")
  private var _sheet = EtlParameter[String](nodeAttribute = "sheet", configAttribute = "dw.datastore."+name+".sheet", defaultValue = defaultSheet)
  private var _colStart = EtlParameter[String](nodeAttribute = "colStart", configAttribute = "dw.datastore."+name+".colStart", defaultValue = defaultColStart)
  private var _rowStart = EtlParameter[String](nodeAttribute = "rowStart", configAttribute = "dw.datastore."+name+".rowStart", defaultValue = defaultRowStart)

  private var _columns = EtlChilds[ColumnBasic]()
  

  def store = _parent.get
  def columns : Seq[Column] = _columns
  def spreadsheetId = _spreadsheetId.value
  def sheet = _sheet.value 
  def colStart = _colStart.value
  def rowStart = _rowStart.value

  override def toString = toXml().toString()

  override def hasSource(): Boolean = _source.isDefined
  
	def canRead = true
	
  def read() : DataSource = store.createDataSource(this)
	
	def canWrite = true
	
	def write() : DataSink = store.createDataSink(this)
	
  def dropTableIfExists() : Table = ???
  
  def createTable() : Table = ???

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    columns map { _.name } mkString ", "
  }
}
