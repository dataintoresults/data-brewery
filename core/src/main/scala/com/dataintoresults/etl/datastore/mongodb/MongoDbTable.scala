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

package com.dataintoresults.etl.datastore.mongodb

import java.io.File
import java.util.Arrays
import scala.collection.JavaConversions._

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

class MongoDbTable extends EtlTable with Table {

  private val _parent = EtlParent[MongoDbStore]()

  private val _columns = EtlChilds[ColumnBasic]()

  def store = _parent.get

  def columns : Seq[Column] = _columns

  override def toString = toXml().toString()

	def canRead = true
	
  def read() : DataSource = store.createDataSource(this)
	
	def canWrite = false
	
	def write() : DataSink = throw new RuntimeException("write not implemented for MongoDbTable")
	
  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for MongoDbTable")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for MongoDbTable")

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    columns map { _.name } mkString ", "
  }
}
