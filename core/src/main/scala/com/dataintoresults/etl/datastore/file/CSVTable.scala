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

package com.dataintoresults.etl.datastore.file

import java.nio.file.{Files, Path, Paths}
import java.io.{InputStreamReader, BufferedInputStream, FileInputStream}
import java.io.{OutputStreamWriter, BufferedOutputStream, FileOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import play.api.Logger

import com.dataintoresults.etl.core.{DataSource, DataSink, Table, Column}
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.core.Source

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core.{EtlChilds, EtlTable, EtlParent, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class CSVTable extends FlatFileTable {
  private val logger = Logger(this.getClass)

  private val _columns = EtlChilds[ColumnBasic]()
  private val _location = EtlParameter[String](nodeAttribute="location", configAttribute="dw.datastore."+store.name+"."+name+".location")
  private val _locationSeparator = EtlParameter[String](nodeAttribute="locationSeparator", 
    configAttribute="dw.datastore."+store.name+"."+name+".locationSeparator", defaultValue="|")
  private val _locale = EtlParameter[String](nodeAttribute="locale", configAttribute="dw.datastore."+store.name+"."+name+".locale", defaultValue= "UTF-8")
  private val _compression = EtlParameter[String](nodeAttribute="compression", configAttribute="dw.datastore."+store.name+"."+name+".compression", defaultValue= "none")
  private val _delimiter = EtlParameter[String](nodeAttribute="delimiter", configAttribute="dw.datastore."+store.name+"."+name+".delimiter", defaultValue= "\t")
  private val _header = EtlParameter[String](nodeAttribute="header", configAttribute="dw.datastore."+store.name+"."+name+".header", defaultValue= "true")
  private val _newline = EtlParameter[String](nodeAttribute="newline", configAttribute="dw.datastore."+store.name+"."+name+".newline", defaultValue= "\n")
  private val _quote = EtlParameter[String](nodeAttribute="quote", configAttribute="dw.datastore."+store.name+"."+name+".quote", defaultValue= "\"")
  private val _quoteEscape = EtlParameter[String](nodeAttribute="quoteEscape", configAttribute="dw.datastore."+store.name+"."+name+".quoteEscape", defaultValue= "\\")
  private val _comment = EtlParameter[String](nodeAttribute="comment", configAttribute="dw.datastore."+store.name+"."+name+".comment", defaultValue= "#")
  

  def location = _location.value
  def locationSeparator = _locationSeparator.value
  def locale = _locale.value
  def compression = _compression.value
  def delimiter = _delimiter.value.head
  def header = if(_header.value == "true") true else false
  def newline = _newline.value
  def quote = _quote.value.head
  def quoteEscape = _quoteEscape.value.head
  def comment = _comment.value.head
  
  private val bufferSize: Int = 1*1024*1024
  
  def columns : Seq[Column] = _columns
  
  override def toString = s"CSVTable[${name}]"

	def canRead = true
	
  def read() : DataSource = {
    val filesList = FileStoreHelper.listFiles(store.location, location, locationSeparator)

    logger.debug(s"${store.name}.${name}: Finding ${filesList.size} files that match the pattern ${store.location} + ${location}.")

    new DataSource {
      private var currentDataSource: DataSource = null
      private val fileIterator = filesList.iterator

      def close(): Unit = {}

      def hasNext: Boolean = {
        if(currentDataSource == null || !currentDataSource.hasNext) {
          if(fileIterator.hasNext) {
            val nextFile = fileIterator.next
            logger.debug(s"${store.name}.${name}: Parsing ${nextFile.toString}.")
            currentDataSource = nextDataSource(nextFile)
            currentDataSource.hasNext
          }
          else {
            false
          }
        }
        else {
          true
        }
      }

      def next(): Seq[Any] = currentDataSource.next

      def structure = columns

      private def nextDataSource(filePath: Path): DataSource  = {
        val reader = new InputStreamReader(new BufferedInputStream(
          compression match {
            case "gz" => try {
              new GZIPInputStream(Files.newInputStream(filePath))
            }
            catch {
              case e: java.util.zip.ZipException => throw new RuntimeException(s"Table ${store.name}.${name} : File ${filePath.toString} doesn't appear to be in gzip") 
            }
            case "none" => Files.newInputStream(filePath)
            case _ => throw new RuntimeException(s"Compression mode ${compression} not understood for table ${store.name}.${name} (should be none or gz).") 
          }, bufferSize), locale)
        CSVReader(columns, delimiter, newline, header, quote, quoteEscape, comment).toDataSource(reader)
      }
    }
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
        
    CSVWriter(columns, delimiter, newline, header, quote, quoteEscape, comment).toDataSink(writer)
	}
	
  def dropTableIfExists() : Table = {
    val file = Paths.get(store.location + location)
    Files.deleteIfExists(file)
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