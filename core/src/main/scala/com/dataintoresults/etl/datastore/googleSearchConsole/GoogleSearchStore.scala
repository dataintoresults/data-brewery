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
import java.io.IOException
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDate, ZoneOffset}
import java.util.Arrays


import scala.collection.JavaConverters._
import play.api.Logger
import com.typesafe.config.Config
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.webmasters.Webmasters
import com.google.api.services.webmasters.model.{SearchAnalyticsQueryRequest, SitesListResponse, WmxSite}

import com.google.api.services.webmasters.model.SitesListResponse
import com.google.api.services.webmasters.model.WmxSite

import com.dataintoresults.etl.core.{Column, DataSink, DataSource, DataStore, Table}
import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.DataSetImpl
import com.dataintoresults.util.InHelper._


class GoogleSearchStore extends EtlDatastore with DataStore {
  private val logger: Logger = Logger(this.getClass())



  private val _tables = EtlChilds[GoogleSearchTable]()

	private val _serviceAccountEmail = EtlParameter[String](nodeAttribute="serviceAccountEmail", configAttribute="dw.datastore."+name+".serviceAccountEmail")
	private val _keyFileLocation = EtlParameter[String](nodeAttribute="keyFileLocation", configAttribute="dw.datastore."+name+".keyFileLocation")
	private val _applicationName = EtlParameter[String](nodeAttribute="applicationName", configAttribute="dw.datastore."+name+".applicationName")

	def serviceAccountEmail = _serviceAccountEmail.value
	def keyFileLocation = _keyFileLocation.value
	def applicationName = _applicationName.value


  override def toString = s"GoogleSearchStore[${name}]"

  def tables() : Seq[Table] = _tables

  private def getConnection() : GoogleSearch = {
    val conn = new GoogleSearch()
    conn.connectAsServiceAccount(serviceAccountEmail, keyFileLocation, applicationName)
    conn
  }




  def createDataSource(gsTable: GoogleSearchTable) : DataSource = {

    val service = getConnection()


    val iterator = service.query(
      property = gsTable.property,
      columns = gsTable.columns.map(_.name),
      startDate = gsTable.startDate,
      endDate = gsTable.endDate,
      maxRows = 50000
    )

    // We create a DataSource on top of the DataSet
    new DataSource {
      def structure = gsTable.columns.seq

      def hasNext() = iterator.hasNext

      def next() = iterator.next()

      def close() = {}
    }
  }


  def close() : Unit = {
  }



}

object GoogleSearchStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): DataStore = {
    val store = new GoogleSearchStore()
    store.parse(dsXml, config)
    store
  }
}
