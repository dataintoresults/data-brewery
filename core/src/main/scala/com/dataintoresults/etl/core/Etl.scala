/*******************************************************************************
 *
 * Copyright (C) 2018-2019 by Obsidian SAS : https://dataintoresults.com/
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

import com.dataintoresults.etl.explorer.DataStoreExplorer

/**
 * Main trait to operate on a data warehouse.
 */
trait Etl {  
  /*
   *  Model manipulation
   */
	def load(dwXml : scala.xml.Node) : Unit
	
	def clear() : Unit
	
	def save() : scala.xml.Node
	
  def findDataStore(dataStore: String) : DataStore
  
	def addDataStore(dsXml: scala.xml.Node) : Unit
	
	def removeDataStore(dataStoreName: String) : Unit
	
	def addTable(dataStoreName: String, tableXml: scala.xml.Node) : Unit
	
	def removeTable(dataStoreName: String, tableName: String) : Unit
		
	
	
	/*
	 * Operations
	 */
	
	def overseer : Overseer
	
	def previewTableFromDataStore(dataStore: String, table: String, nbRows: Int = 10) : DataSet
	
	def copy(dataStoreFrom: String, tableFrom: String, dataStoreTo: String, tableTo: String) : Unit
	
	def replicate(dataStoreFrom: String, tableFrom: String, dataStoreTo: String, tableTo: String) : Unit
		
	def runModule(moduleId: String) : Unit
	
	def runDataStore(dataStoreName: String) : Unit
	
	def runDataStoreTable(dataStoreName: String, tableName: String) : Unit
	
	def runQuery(store: String, query: String) : DataSet
	
	def exploreDataStore(store: String) : DataStoreExplorer
	
	def close() : Unit
	
	
	/*
	 * Easy meta model manipulation 
	 */
	
	def addGoogleAnalyticsDataStore(name: String, viewId: String) : Unit
  
  def addGoogleAnalyticsTable(dataStore: String, name: String, dimensions: Seq[String], measures: Seq[String]) : Unit
  
	def addSqlDataStore(name: String, backend: String, host: String, database: String, user: String, password: String) : Unit
	
	def addSqlTable(dataStore: String, name: String, schema: String, columns: Seq[(String, String)]) : Unit
}