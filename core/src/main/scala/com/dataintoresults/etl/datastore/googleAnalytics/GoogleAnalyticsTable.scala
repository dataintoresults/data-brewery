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

package com.dataintoresults.etl.datastore.googleAnalytics

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

import com.dataintoresults.etl.core.{EtlChilds, EtlTable, EtlParent, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._


class GoogleAnalyticsTable extends EtlTable with Table {

  private val _parent = EtlParent[GoogleAnalyticsStore]()
  private val _gaColumns = EtlChilds[GoogleAnalyticsColumn]()
  private val _viewId = EtlParameter[String](nodeAttribute="viewId", configAttribute="dw.datastore."+store.name+"."+name+".viewId", defaultValue = Some(null))
  private val _startDate = EtlParameter[String](nodeAttribute="startDate", configAttribute="dw.datastore."+store.name+"."+name+".startDate", defaultValue="7daysAgo")
  private val _endDate = EtlParameter[String](nodeAttribute="endDate", configAttribute="dw.datastore."+store.name+"."+name+".endDate", defaultValue="yesterday")

  def store = _parent.get
  def gaColumns = _gaColumns
  def viewId : String = {
    if(_viewId.value == null || _viewId.value == "") {
      store.viewId match {
        case Some(id) => id
        case None => throw new RuntimeException(s"Table ${store.name}.${name} doesn't have a viewId (and neither ${store.name}).")
      }
    }
    else _viewId.value
  }
  def startDate = _startDate.value
  def endDate = _endDate.value

  def columns : Seq[Column] = _gaColumns

  override def toString = s"GoogleAnalyticsTable[${name},${startDate},${endDate},(${columnsAsString()})]"

	def canRead = true
	
  def read() : DataSource = {
		store.createDataSource(this)
	}
	
	def canWrite = false
	
	def write() : DataSink = throw new RuntimeException("You cannot write to a Google Analytics table.")
	
  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for GoogleAnalyticsTable")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for GoogleAnalyticsTable")

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    gaColumns map { _.name } mkString ", "
  }
}
