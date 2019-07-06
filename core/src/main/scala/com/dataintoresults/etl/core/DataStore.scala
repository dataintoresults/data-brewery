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
 * Represent a backend where are tables.
 */
trait DataStore {
	def name: String
	def name_ (value:String): Unit

	def tables : Seq[Table] 
	
	def addTable(spec : scala.xml.Node) : Unit = ???

	def removeTable(tableName: String) : Unit = ???
	
 	def test() : Boolean = {
 		tables.foreach({ table =>
 			table.test()
 			})
 		true
 	}
		
	def table(name: String) : Option[Table] = tables.find(_.name == name)
	
	def close() : Unit
	
  def toXml() : scala.xml.Node
}

