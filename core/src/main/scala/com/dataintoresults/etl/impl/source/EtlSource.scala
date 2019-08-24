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

package com.dataintoresults.etl.impl.source


import scala.xml.{Attribute, Elem, Node, Null}
import com.typesafe.config.Config

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core._
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.etl.impl._



abstract class EtlSource extends EtlElement(EtlSource.label) with Source {
	protected val _type = EtlParameter[String](nodeAttribute = "type")
	protected val _condition = EtlOptionalChild[EtlCondition]()
  
  def sourceType: String = _type.value

  def condition: Option[EtlCondition] = _condition.toOption

  /**
   * Import the source data in a staging table inside the module
   * and provide a SQL select query to get the data.
   * Can return None is all is taken care of by the source.
   */
  def processOnModule(etl: EtlImpl, toModule: Module, toTable: Table): Option[String]
  
  /**
   * Create a DataSource that feed the source data.
   * Can return None is all is taken care of by the source.
   */
  def processOnDataStore(etl: EtlImpl, toDatastore: DataStore, toTable: Table): Option[DataSource]
}



class SourceQuery extends EtlSource {  
	private val _query = EtlParameter[String](nodeAttribute = "query", cdata=true)

	def query = _query.value 

  override def processOnModule(etl: EtlImpl, toModule: Module, toTable: Table) = Some(query)
  
  override def processOnDataStore(etl: EtlImpl, ds: DataStore, dsTable: Table): Option[DataSource] =
    throw new RuntimeException("Processing a data store table with a query source is not supported yet.")
}

class SourceScript extends EtlSource {  
	private val _script = EtlParameter[String](nodeAttribute = "script", cdata=true)

	def script = _script.value 

  override def processOnModule(etl: EtlImpl, toModule: Module, toTable: Table) = ???
  
  override def processOnDataStore(etl: EtlImpl, ds: DataStore, dsTable: Table): Option[DataSource] = ???
}

class SourceModule extends EtlSource {  
	private val _module = EtlParameter[String](nodeAttribute = "module")
	private val _table = EtlParameter[String](nodeAttribute = "table")

  def module = _module.value
  def table = _table.value

  override def processOnModule(etl: EtlImpl, toModule: Module, toTable: Table) = {
    val srcModule = etl.findModule(module).asInstanceOf[ModuleImpl]
    val srcTable = srcModule.moduleTable(table) getOrElse
	    (throw new RuntimeException(s"Unable to locate a table ${table} in module ${module} to create the table ${toModule.name}.${toTable.name}")) 
	    
    s"select * from ${srcTable.schema}.${srcTable.name}"
  }
  
  override def processOnDataStore(etl: EtlImpl, ds: DataStore, dsTable: Table): Option[DataSource] = {	
    val srcModule = etl.findModule(module).asInstanceOf[ModuleImpl]	  
    val srcTable = srcModule.moduleTable(table) getOrElse
      (throw new RuntimeException(s"Unable to locate a table ${table} in module ${module} to process the table ${ds.name}.${dsTable.name}"))
	       
    srcTable.read()
  }
}

class SourceDataStore extends EtlSource {  
	private val _dataStore = EtlParameter[String](nodeAttribute = "datastore")
	private val _table = EtlParameter[String](nodeAttribute = "table")

  def dataStore = _dataStore.value
  def table = _table.value

  def this(dataStore: String, table: String) = {
    this()
    _type.set(Source.dataStore)
    _dataStore.set(dataStore)
    _table.set(table)
  }
  
  override def processOnModule(etl: EtlImpl, toModule: Module, toTable: Table) = {
    val ds = etl.findDataStore(dataStore)
	        
	  val srcTable = ds.table(table) getOrElse
	   (throw new RuntimeException(s"Unable to locate a table ${table} in data source ${dataStore} to create the table ${toModule.name}.${toTable.name}")) 
	        


    condition.map{ _.isValidated(etl) }.getOrElse(true) match {
      case false => toModule.dataStore.asInstanceOf[SqlStore].createEmptyQuery(srcTable.columns)
      case true => {
        val source = srcTable.read()
        if(!toModule.dataStore.isInstanceOf[SqlStore])
          throw new RuntimeException(s"The data store ${toModule.dataStore.name} is not able to process SQL query for sourcing of table ${toModule.name}.${toTable.name}")

        val sqlStore = toModule.dataStore.asInstanceOf[SqlStore]
        val schema = toModule.schema

        // Table data with be cached on the module database under this name
        val stagingTableName = dataStore + "_" + table +"_staging"
        sqlStore.dropTableIfExists(schema, stagingTableName);
        sqlStore.createTable(schema, stagingTableName, source.structure)
        val sink = sqlStore.createDataSink(schema, stagingTableName, source.structure)    	    
        etl.overseer.runJob(source, sink)
              
        s"select * from ${schema}.${stagingTableName}"
      }
    }
  }
  
  override def processOnDataStore(etl: EtlImpl, toDs: DataStore, toTable: Table): Option[DataSource] = {
    val srcDatastore = etl.findDataStore(dataStore)
	  val srcTable = srcDatastore.table(table) getOrElse
	    (throw new RuntimeException(s"Unable to locate a table ${table} in data source ${dataStore} to process the table ${toDs.name}.${toTable.name}")) 
	        
    srcTable.read()
  }

}

class SourceDataStoreQuery extends EtlSource {  
	private val _dataStore = EtlParameter[String](nodeAttribute = "datastore")
	private val _query = EtlParameter[String](nodeAttribute = "query", cdata=true)

  def dataStore = _dataStore.value
  def query = _query.value

  override def processOnModule(etl: EtlImpl, toModule: Module, toTable: Table) = {
    val ds = etl.findDataStore(dataStore)
	  
	  // Check that the datas tore is SQL compliant
	  if(!ds.isInstanceOf[SqlStore])
	    throw new RuntimeException(s"The data store ${ds.name} is not able to process SQL query for sourcing of table ${toModule.name}.${toTable.name}")
	   
	  val sqlDs = ds.asInstanceOf[SqlStore]

	  val source = sqlDs.createDataSource(query)

    if(!toModule.dataStore.isInstanceOf[SqlStore])
	    throw new RuntimeException(s"The data store ${toModule.dataStore.name} is not able to process SQL query for sourcing of table ${toModule.name}.${toTable.name}")

    val sqlStore = toModule.dataStore.asInstanceOf[SqlStore]
    val schema = toModule.schema

    // Table data with be cached on the module database under this name
	  val stagingTableName = dataStore +"_staging"
    sqlStore.dropTableIfExists(schema, stagingTableName);
    sqlStore.createTable(schema, stagingTableName, source.structure)
    val sink = sqlStore.createDataSink(schema, stagingTableName, source.structure)    	    
    etl.overseer.runJob(source, sink)
	        
	  s"select * from ${schema}.${stagingTableName}"
  }
  
  override def processOnDataStore(etl: EtlImpl, toDs: DataStore, toTable: Table): Option[DataSource] = {
    val ds = etl.findDataStore(dataStore)
	        
	  // Check that the datastore is SQL compliant
	  if(!ds.isInstanceOf[SqlStore])
	    throw new RuntimeException(s"The data store ${dataStore} is not able to process SQL query for sourcing of table ${toDs.name}.${toTable.name}")
	        
	  val sqlDs = ds.asInstanceOf[SqlStore]
	    
    sqlDs.createDataSource(query)	
  }

 
}



object EtlSource extends EtlElementFactory {
  def label: String = "source"

  def parse(node: Node, config: Option[Config] = None, context: String = "", parent: AnyRef = null): EtlSource = {
    val tpe = (node \@? "type")
      .getOrElse(throw new RuntimeException(s"No value for parameter type for source element. $context"))
    val src = tpe match {
      case Source.query => new SourceQuery()
      case Source.module => new SourceModule()
      case Source.dataStore => new SourceDataStore()
      case Source.dataStoreQuery => new SourceDataStoreQuery()
    } 

    src.parse(node, config, context).asInstanceOf[EtlSource]
  }
}