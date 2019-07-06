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

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.etl.core.Module
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.impl.source.SourceDataStore
import com.dataintoresults.etl.core.Etl
import com.dataintoresults.etl.core.ReplicateDataStore
import com.dataintoresults.util.XmlHelper._

class ModuleImpl(private var _etl: Etl, private var _datastore : SqlStore) extends Module {
	private var _name = "";
 	private var _tables : Seq[ModuleTable] = Nil
 	private var _replicateDataStore : Seq[ReplicateDataStore] = Nil
 	 	
	def name = _name
 	def name_=(value: String): Unit = _name = value
	
 	def dataStore: DataStore = _datastore
 	def sqlStore: SqlStore = _datastore
 	
 	def schema: String = name

	def tables : Seq[Table]  = {
	  ( replicateDataStore flatMap { rep => replicationTables(rep) } ) ++ _tables
	}
	
	def moduleTables : Seq[ModuleTable]  = {
	  ( replicateDataStore flatMap { rep => replicationTables(rep) } ) ++ _tables
	}
	
	
	def moduleTable(name: String) : Option[ModuleTable] = moduleTables.find(_.name == name)
	
	
	def replicateDataStore : Seq[ReplicateDataStore] = _replicateDataStore
	
	
 	def fromXml(modXml: scala.xml.NodeSeq) : Unit = {
		_name = (modXml \ "@name").toString
		_tables = (modXml \ "table").map( { tXml =>
				val table = new ModuleTable
				table.parse(tXml, parent=this)
				table
			})
 	
		_replicateDataStore = (modXml \ "replicate").map( { rXml =>
		    val dsStr = (rXml \ "@datastore").toString
		    val ds = try { _etl.findDataStore(dsStr) } 
		      catch { case e : Exception => throw new Exception(s"Data store '${dsStr}' not found for replication in module '${_name}'") }
		    val strategy = rXml \@? "strategy" getOrElse null
				val replicate = new ReplicateDataStoreImpl(_etl, ds, strategy)
		    replicate
			})
 	}
 	
			
  def toXml() : scala.xml.Node = {
    <module name={name} datastore={dataStore.name}>
			{ _replicateDataStore map { _.toXml() } } 
			{ _tables map { _.toXml() } } 
		</module>
  }
  
  
  /** 
   *  private functions 
   */
  
  
  private def replicationTables(replicate: ReplicateDataStore) : Seq[ModuleTable] = {
    replicate.tables map { table =>
      val columns = table.columns.map { c => new ColumnBasic(c.name, c.colType)}
      val name = table.name
      val mergeStrategy = replicate.strategy match {
        case "rebuild" => ModuleTable.Rebuild
        case "overwrite" => ModuleTable.Overwrite
        case "trackChange" => ModuleTable.TrackChange
      }
      val source = new SourceDataStore(replicate.dataStore.name, table.name)
      new ModuleTable(this, name, columns, source, mergeStrategy)
    }
  }
}