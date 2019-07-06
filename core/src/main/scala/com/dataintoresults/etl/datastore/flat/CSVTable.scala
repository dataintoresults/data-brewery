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

package com.dataintoresults.etl.datastore.flat

import java.io.{File}
import java.io.{InputStreamReader, BufferedInputStream, FileInputStream}
import java.io.{OutputStreamWriter, BufferedOutputStream, FileOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}


import com.dataintoresults.etl.core.{DataSource, DataSink, Table, Column}
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.core.Source

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core.{EtlChilds, EtlTable, EtlParent, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class CSVTable extends EtlTable {

  private val _parent = EtlParent[FlatFileStore]()

  private val _columns = EtlChilds[ColumnBasic]()
  private val _location = EtlParameter[String](nodeAttribute="location")
  private val _locale = EtlParameter[String](nodeAttribute="locale", defaultValue= "UTF-8")
  private val _compression = EtlParameter[String](nodeAttribute="compression", defaultValue= "none")
  
  def store = _parent.get

  def location = _location.value
  def locale = _locale.value
  def compression = _compression.value
  
  private val bufferSize: Int = 1*1024*1024
  
  def columns : Seq[Column] = _columns
  
  override def toString = s"CSVTable[${name}]"

	def canRead = true
	
  def read() : DataSource = {
	  val reader = new InputStreamReader(new BufferedInputStream(
	      compression match {
	        case "gz" => try {
	          new GZIPInputStream(new FileInputStream(store.location + location))
	        }
	        catch {
	          case e: java.util.zip.ZipException => throw new RuntimeException(s"Table ${store.name}.${name} : File ${store.location + location} doesn't appear to be in gzip") 
	        }
	        case "none" => new FileInputStream(store.location + location)
	        case _ => throw new RuntimeException(s"Compression mode ${compression} not understood for table ${store.name}.${name} (should be none or gz).") 
	      }, bufferSize), locale)
    CSVReader(columns = columns).toDataSource(reader)
	}
	
	def canWrite = true
	
	def write() : DataSink = {	  
	  val writer = new OutputStreamWriter(new BufferedOutputStream(
	      compression match {
	        case "gz" => try {
	          new GZIPOutputStream(new FileOutputStream(store.location + location), false)
	        }
	        catch {
	          case e: java.util.zip.ZipException => throw new RuntimeException(s"Table ${store.name}.${name} : File ${store.location + location} doesn't appear to be in gzip") 
	        }
	        case "none" => new FileOutputStream(store.location + location, false)
	        case _ => throw new RuntimeException(s"Compression mode ${compression} not understood for table ${store.name}.${name} (should be none or gz).") 
	      }, bufferSize), locale)
    CSVWriter(columns = columns).toDataSink(writer)
	}
	
  def dropTableIfExists() : Table = {
    val file = new File(store.location + location)
    if(file.exists) 
      file.delete()
    this
  }

  def createTable() : Table = {
    write().close()
    this
  }

  private def columnsAsString() : String = {
    columns map { _.name } mkString ", "
  }
}