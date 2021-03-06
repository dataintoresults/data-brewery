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

package com.dataintoresults.etl.datastore.odoo

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
import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core.{EtlParent, EtlChilds, EtlTable, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class OdooTable extends EtlTable with Table {

  private val _parent = EtlParent[OdooStore]()

  private val _odooModel = EtlParameter[String](nodeAttribute="odooModel")

  private var _columns = EtlChilds[OdooColumn]()

  def store = _parent.get

  def odooModel = _odooModel.value
  
  def columns : Seq[Column] = _columns
  def odooColumns : Seq[OdooColumn] = _columns

  override def toString = toXml().toString()

	def canRead = true
	
  def read() : DataSource = store.createDataSource(this)
	
	def canWrite = false
	
	def write() : DataSink = throw new RuntimeException("write not implemented for OdooTable")
	
  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for OdooTable")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for OdooTable")

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    columns map { _.name } mkString ", "
  }
}
