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

/**
 * A module is a structured subspace in a data store.
 */
trait Module {

	def name : String
	def name_=(value:String):Unit
	
	/**
	 * The data store where this module is located.
	 */
	def dataStore : DataStore
	
	def tables : Seq[Table] 
	
	/**
	 * schema that contains this module in the data store.
	 */
	def schema : String
	
	def replicateDataStore : Seq[ReplicateDataStore]
	
	def table(name: String) : Option[Table] = tables.find(_.name == name)
	
  def toXml() : scala.xml.Node
}