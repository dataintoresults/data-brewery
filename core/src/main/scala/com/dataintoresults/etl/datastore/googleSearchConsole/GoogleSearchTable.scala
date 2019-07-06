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

package com.dataintoresults.etl.datastore.googleSearchConsole

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
import com.dataintoresults.etl.core.{Column, DataSink, DataSource, DataStore, EtlChilds, EtlDatastore, EtlParameter, EtlParent, EtlTable, Table}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.ColumnBasic



class GoogleSearchTable extends EtlTable with Table {

  var gsColumns  = EtlChilds[GoogleSearchColumn]()

  private val _property = EtlParameter[String](nodeAttribute = "property")
  private val _startDate = EtlParameter[String](nodeAttribute = "startDate")
  private val _endDate = EtlParameter[String](nodeAttribute = "endDate")

  def property = _property.value
  def startDate = _startDate.value
  def endDate = _endDate.value

  def columns = gsColumns

  val _parent = EtlParent[GoogleSearchStore]()

  def store = _parent.get


  override def toString = s"GoogleSearchTable[${name},${startDate},${endDate},(${columnsAsString()})]"

  def canRead = true

  def read() : DataSource = store.createDataSource(this)

  def canWrite = false

  def write() : DataSink = throw new RuntimeException("You cannot write to a Google Search table.")

  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for GoogleSearchTable")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for GoogleSearchTable")

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    gsColumns map { _.name } mkString ", "
  }
}
