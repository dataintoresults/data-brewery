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

package com.dataintoresults.etl.datastore.sql

import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.impl.ColumnBasic

import com.dataintoresults.etl.core.{EtlTable, EtlParameter, EtlChilds, EtlParent}
import com.dataintoresults.etl.core.EtlParameterHelper._

class SqlTable extends EtlTable with Table {
  private val _store = EtlParent[SqlStore]()

   private val _schema = EtlParameter[String](nodeAttribute="schema", defaultValue="")

  private val _columns = EtlChilds[ColumnBasic]()

   def store = _store.get
   def schema = _schema.value      
   def columns : Seq[Column] = _columns.value

  def this(store: SqlStore, name: String, schema: String, columns: Seq[ColumnBasic]) = {
    this()
    _store.set(store)
    _name.set(name)
    _schema.set(schema)
    _columns.set(columns)
  }

   override def toString = s"SqlTable[${name},${schema}]"
         
  def canRead = true
  
  def read() : DataSource = store.createDataSource(this)
  
  def canWrite = true
  
  def write() : DataSink = store.createDataSink(this)
  
  
  def dropTableIfExists() : Table = {
    store.dropTableIfExists(schema, name)
    this
  }

  def createTable() : Table = {
    store.createTable(schema, name, columns)
    this
  }

}