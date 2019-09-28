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

package com.dataintoresults.etl.datastore.bitmovin


import com.dataintoresults.etl.core.{Column, DataSink, DataSource, DataStore, EtlChilds, EtlDatastore, EtlParameter, EtlParent, EtlTable, Table}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.impl.FilterFormula



class BitmovinTable extends EtlTable with Table {

  val bColumns  = EtlChilds[BitmovinColumn]()

  private val _startDate = EtlParameter[String](nodeAttribute = "startDate")
  private val _endDate = EtlParameter[String](nodeAttribute = "endDate")
  private val _filters = EtlChilds[FilterFormula]()

  def startDate = _startDate.value
  def endDate = _endDate.value
  def filters = _filters.value

  def columns = bColumns

  val _parent = EtlParent[BitmovinStore]()

  def store = _parent.get


  override def toString = s"GoogleSearchTable[${name},${startDate},${endDate},(${columnsAsString()})]"

  def canRead = true

  def read() : DataSource = store.createDataSource(this)

  def canWrite = false

  def write() : DataSink = throw new RuntimeException("You cannot write to a Bitmovin table.")

  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for Bitmovin")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for Bitmovin")

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    columns map { _.name } mkString ", "
  }
}
