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

package com.dataintoresults.etl.impl

import scala.xml._

import com.typesafe.config.Config

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.Module
import com.dataintoresults.etl.core.Source

import com.dataintoresults.etl.core.{EtlElement, EtlParameter, EtlParent, EtlChilds, EtlOptionalChild}
import com.dataintoresults.etl.core.EtlParameterHelper._

import com.dataintoresults.etl.impl.source.EtlSource

import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.etl.datastore.sql.SqlTable


class ModuleTable extends SqlTable {
	private val _module = EtlParent[ModuleImpl]()
	private val _columns = EtlChilds[ColumnBasic]()

	private val _businessKeysString = EtlParameter[String](nodeAttribute="businessKeys", defaultValue="")

	def businessKeys : Seq[String] = _businessKeysString.value.split(",")

 	private val _mergeStrategy = EtlParameter[String](nodeAttribute="strategy", defaultValue="rebuild")

 	def mergeStrategy: ModuleTable.MergeStrategy = _mergeStrategy.value.toLowerCase() match {
    case "rebuild" => ModuleTable.Rebuild
    case "overwrite" => ModuleTable.Overwrite
    case "trackchange" => ModuleTable.TrackChange
  }
 	
	def module = _module.get
	override def schema = module.schema

 	override def columns : Seq[Column] = _columns
 	def columns_=(value : Seq[ColumnBasic]) : Unit = _columns.set(value)
 	
 	def this(module: ModuleImpl, name: String, columns : Seq[ColumnBasic], source : EtlSource = null, 	    
 	    mergeStrategy : ModuleTable.MergeStrategy = ModuleTable.Rebuild) = {
		this()
	  _module.set(module)
	  _name.set(name)
	  _columns.set(columns)
	  _source.set(if(source == null) Seq.empty[EtlSource] else Seq(source))
	  _mergeStrategy.set(mergeStrategy match {
			case ModuleTable.Rebuild => "rebuild"
			case ModuleTable.Overwrite => "overwrite" 
			case ModuleTable.TrackChange => "trackchange"
		})
	}
  
 	
	override def canRead = true
	
  override def read() : DataSource = {
	  val store = module.dataStore.asInstanceOf[SqlStore]
	  store.createDataSource(this)	  
	}
	
	override def canWrite = true
	
	override def write() : DataSink = {
	  val store = module.dataStore.asInstanceOf[SqlStore]
	  store.createDataSink(this)  
	}
	
  override def dropTableIfExists() : Table = {
	  val sqlDataStore = module.dataStore.asInstanceOf[SqlStore]
	  sqlDataStore.dropTableIfExists(schema, name)
	  this
	}

  override def createTable() : Table = {
	  val sqlDataStore = module.dataStore.asInstanceOf[SqlStore]
	  sqlDataStore.createTable(schema, name, columns)
	  this
	}
  			
  override def parse(node: Node, config: Option[Config] = None, 
		context: String = "", parent: AnyRef = null): EtlElement = {
		val r = super.parse(node, config, context, parent)
    
    if(businessKeys.length == 0 && mergeStrategy == ModuleTable.Overwrite)
      throw new RuntimeException(s"You need to have a businessKeys attribute in for module table ${module.name}.${name} to use the overwrite strategy")
    else if(businessKeys.length == 0 && mergeStrategy == ModuleTable.TrackChange)
      throw new RuntimeException(s"You need to have a businessKeys attribute in for module table ${module.name}.${name} to use the track changes strategy")

		r
  }

}

object ModuleTable {
 	sealed trait MergeStrategy
  case object Rebuild extends MergeStrategy
  case object Overwrite extends MergeStrategy
  case object TrackChange extends MergeStrategy
}
