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

import java.io.File
import java.util.Arrays

import scala.collection.JavaConversions._

import com.dataintoresults.etl.core.{Column, DataSink, DataSource, DataStore, EtlChilds, EtlDatastore, EtlParameter, EtlParent, EtlTable, Table}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.ColumnBasic



class HubspotTable extends EtlTable with Table {

  private val _hColumns  = EtlChilds[HubspotColumn]()

  private val _endpoint  = EtlParameter[String](nodeAttribute = "endpoint")
  private val _root  = EtlParameter[String](nodeAttribute = "root")
  private val _hasMore  = EtlParameter[String](nodeAttribute = "hasMore", defaultValue="")
  private val _offset  = EtlParameter[String](nodeAttribute = "offset", defaultValue="")
  private val _offsetUrl  = EtlParameter[String](nodeAttribute = "offsetUrl", defaultValue="")
  private val _offset2  = EtlParameter[String](nodeAttribute = "offset2", defaultValue="")
  private val _offset2Url  = EtlParameter[String](nodeAttribute = "offset2Url", defaultValue="")
  private val _rowsPerCall  = EtlParameter[Int](nodeAttribute = "rowsPerCall", defaultValue=100)
  private val _rowsPerCallUrl  = EtlParameter[String](nodeAttribute = "rowsPerCallUrl", defaultValue="")
  private val _urlAttributes  = EtlParameter[String](nodeAttribute = "urlAttributes", defaultValue="")

  def endpoint = _endpoint.value
  def root = _root.value
  def hasMore = _hasMore.value
  def offset = _offset.value
  def offsetUrl = _offsetUrl.value
  def offset2 = _offset2.value
  def offset2Url = _offset2Url.value
  def has2Offsets = !_offset2.value.equals("")
  def rowsPerCall = _rowsPerCall.value
  def rowsPerCallUrl = _rowsPerCallUrl.value
  def urlAttributes = _urlAttributes.value

  def columns = _hColumns

  def hubspotColumns = _hColumns

  val _parent = EtlParent[HubspotStore]()

  def store = _parent.get


  override def toString = s"HubspotTable[${name},$endpoint,(${columnsAsString()})]"

  def canRead = true

  def read() : DataSource = store.createDataSource(this)

  def canWrite = false

  def write() : DataSink = throw new RuntimeException("You cannot write to a Hubspot table.")

  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for Hubspot")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for Hubspot")

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    hubspotColumns map { _.name } mkString ", "
  }
}
