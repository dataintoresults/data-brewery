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
 * Represents a set of data stores and a set of modules.s
 */
trait DataWarehouse {
	
	def datastores : Seq[DataStore]
	
	def datastores_=(datastores: Seq[DataStore]) : Unit
	
	def addDatastore(datastore : DataStore) : Unit 
	
	def removeDatastore(name : String) : Unit
		
	def modules : Seq[Module]
	
	def modules_=(modules: Seq[Module]) : Unit
	
	def addModule(module : Module) : Unit
	
	def deleteModule(name : String) : Unit
	
	def close() : Unit
	
	def test() : Boolean 
}