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

import play.api.Logger
   
import com.typesafe.config.Config

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
import com.google.api.services.analyticsreporting.v4.model.Report

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.impl.DataSetImpl

import com.dataintoresults.etl.core.{Column, DataSink, DataSource, DataStore, Table}
import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class GoogleAnalyticsStore extends EtlDatastore with DataStore {
  private val logger: Logger = Logger(this.getClass())
	

  private val _tables = EtlChilds[GoogleAnalyticsTable]()

	private val _serviceAccountEmail = EtlParameter[String](nodeAttribute="serviceAccountEmail", configAttribute="dw.datastore."+name+".serviceAccountEmail")
	private val _keyFileLocation = EtlParameter[String](nodeAttribute="keyFileLocation", configAttribute="dw.datastore."+name+".keyFileLocation")
	private val _applicationName = EtlParameter[String](nodeAttribute="applicationName", configAttribute="dw.datastore."+name+".applicationName")
	private val _viewId = EtlParameter[String](nodeAttribute="viewId", configAttribute="dw.datastore."+name+".viewId")

	def serviceAccountEmail = _serviceAccountEmail.value
	def keyFileLocation = _keyFileLocation.value
	def applicationName = _applicationName.value
	def viewId = _viewId.value

	def tables() : Seq[Table] = _tables

	override def toString = s"GoogleAnalyticsStore[${name},${viewId}]"
	
	def createDataSource(gaTable: GoogleAnalyticsTable) : DataSource = {
	  
		val gaStore = this

		val service = new GoogleAnalytics()

		service.connectAsServiceAccount(serviceAccountEmail, keyFileLocation, applicationName)

		val iterator = service.query(viewId, 
			gaTable.gaColumns.filter(_.gaType == "measure").map(_.gaName),			
			gaTable.gaColumns.filter(_.gaType == "dimension").map(_.gaName),
			gaTable.startDate, gaTable.endDate)

		// We create a DataSource on top of the DataSet
		new DataSource {
		  private val it = iterator

			private var batch = Seq.empty[Seq[Object]]
			private var nextPageToken = Option.empty[String]
		  
		  def structure = gaTable.columns
		  
		  def hasNext() = it.hasNext()
		  
		  def next() = it.next()
		  
		  def close() = { }
		}
	}
		
	def close() : Unit = {
	}
	

}

object GoogleAnalyticsStore {
	def fromXml(dsXml : scala.xml.Node, config: com.typesafe.config.Config) : DataStore = {
		val store = new GoogleAnalyticsStore()
		store.parse(dsXml, config)
		store
	}
}