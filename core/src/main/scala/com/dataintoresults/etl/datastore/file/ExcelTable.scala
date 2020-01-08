/*******************************************************************************
 *
 * Copyright (C) 2019 by Obsidian SAS : https://dataintoresults.com/
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

import java.nio.file.{Paths, Path}

import play.api.Logger

import com.dataintoresults.etl.core.{DataSource, DataSink, Table, Column}
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.core.Source

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core.{EtlChilds, EtlTable, EtlParent, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class ExcelTable extends FlatFileTable {
  private val logger = Logger(this.getClass)

  private val _columns = EtlChilds[ColumnBasic]()
  private val _location = EtlParameter[String](nodeAttribute="location", configAttribute="dw.datastore."+store.name+"."+name+".location")
  private val _locationSeparator = EtlParameter[String](nodeAttribute="locationSeparator", 
    configAttribute="dw.datastore."+store.name+"."+name+".locationSeparator", defaultValue="|")
  private val _sheet = EtlParameter[String](nodeAttribute="sheet", configAttribute="dw.datastore."+store.name+"."+name+".sheet")
  private val _colStart = EtlParameter[String](nodeAttribute="colStart", configAttribute="dw.datastore."+store.name+"."+name+".colStart")
  private val _rowStart = EtlParameter[Int](nodeAttribute="rowStart", configAttribute="dw.datastore."+store.name+"."+name+".rowStart")
  

  def location = _location.value
  def locationSepartor = _locationSeparator.value
  def sheet = _sheet.value
  def colStart = _colStart.value
  def rowStart = _rowStart.value
  
  def columns : Seq[Column] = _columns
  
  override def toString = s"ExcelTable[${name}]"

	def canRead = true
	
  def read() : DataSource = {
    val filesList = FileStoreHelper.listFiles(store.location, location, locationSepartor)

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
        //if(this.tableType = "xlsx")
          XlsxReader(columns, sheet, colStart, rowStart).toDataSource(filePath)
        
      }
    }
	}
	
	def canWrite = true
	
	def write() : DataSink = {	  
    val path = Paths.get(store.location + location)
    XlsxWriter(columns, sheet, colStart, rowStart).toDataSink(path)
	}
	
  def dropTableIfExists() : Table = {
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