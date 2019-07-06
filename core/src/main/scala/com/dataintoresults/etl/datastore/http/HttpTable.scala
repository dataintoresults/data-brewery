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

import java.io.File
import java.util.Arrays
import scala.collection.JavaConversions._

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core.{EtlParent, EtlChilds, EtlTable, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class HttpTable extends EtlTable with Table {
  
  private val defaultFormat : String = "text"
  
  private val _store = EtlParent[HttpStore]()
  def store = _store.get

  private val _columns = EtlChilds[HttpColumn]()
  def columns : Seq[Column] = _columns
  def httpColumns : Seq[HttpColumn] = _columns

  private val _location = EtlParameter[String](nodeAttribute = "location", configAttribute = "dw.datastore."+name+".location")
  private val _format = EtlParameter[String](nodeAttribute = "format", configAttribute = "dw.datastore."+name+".format", defaultValue=defaultFormat)
  private val _csvSeparator = EtlParameter[String](nodeAttribute = "csvSeparator", configAttribute = "dw.datastore."+name+".csvSeparator", defaultValue=",")
  private val _csvQuote = EtlParameter[String](nodeAttribute = "csvQuote", configAttribute = "dw.datastore."+name+".csvQuote", defaultValue="\"")
  private val _csvHeader = EtlParameter[String](nodeAttribute = "csvHeader", configAttribute = "dw.datastore."+name+".csvHeader", defaultValue="true")

  def location = _location.value
  def format = _format.value
  def csvSeparator = _csvSeparator.value
  def csvQuote = _csvQuote.value
  def csvSkipRows = if(_csvHeader.value == "true") 1 else 0
  
  override def toString = toXml().toString()

	def canRead = true
	
  def read() : DataSource = store.createDataSource(this)
	
	def canWrite = false
	
	def write() : DataSink = throw new RuntimeException("You cannot write to a HttpTable.")  

  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for HttpTable")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for HttpTable")

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    columns map { _.name } mkString ", "
  }
}
