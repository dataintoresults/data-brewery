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

package com.dataintoresults.etl.core


trait Source {
  def sourceType: String  
  
  def toXml() : scala.xml.Node
}

object Source {
  final val query = "query"
  final val script = "script"
  final val module = "module"
  final val dataStore = "datastore"
  final val dataStoreQuery = "datastoreQuery"
}
/*
case class SourceQuery(query: String) extends Source {  
  def sourceType: String = Source.query
 
  def toXml() : scala.xml.Node = {
    <source type={sourceType}>
      { scala.xml.PCData(query) } 
    </source>
  }
}

case class SourceModule(module: String, table: String) extends Source {  
  def sourceType: String = Source.module
 
  def toXml() : scala.xml.Node = {
    <source type={sourceType} module={module} table={table}/>
  }
}

case class SourceDataStore(dataStore: String, table: String) extends Source {  
  def sourceType: String = Source.dataStore
 
  def toXml() : scala.xml.Node = {
    <source type={sourceType} datastore={dataStore} table={table}/>
  }
}

case class SourceDataStoreQuery(dataStore: String, query: String) extends Source {  
  def sourceType: String = Source.dataStoreQuery
 
  def toXml() : scala.xml.Node = {
    <source type={sourceType} datastore={dataStore}>
      { scala.xml.PCData(query) } 
		</source>
  }
}*/