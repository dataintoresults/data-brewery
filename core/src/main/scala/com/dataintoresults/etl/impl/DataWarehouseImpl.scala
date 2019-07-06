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
import com.dataintoresults.etl.core.DataWarehouse
import com.dataintoresults.etl.core.Module



class DataWarehouseImpl extends DataWarehouse {
	private var _datastores: Seq[DataStore] = Nil
	private var _modules: Seq[Module] = Nil

	
	def datastores = _datastores
	
	def datastores_=(datastores: Seq[DataStore]) {
    _datastores = datastores
  }
	
	def addDatastore(datastore : DataStore) : Unit = {
		_datastores =  _datastores :+ datastore
	}
	
	def removeDatastore(name : String) : Unit = {
		_datastores = _datastores filter { _.name != name }
	}
	
	
	def modules = _modules
	
	def modules_=(modules: Seq[Module]) {
    _modules = modules
  }
	
	def addModule(module : Module) : Unit = {
		_modules =  _modules :+ module
	}	
	
	def deleteModule(name : String) : Unit = {
		_modules = _modules filter { _.name != name }
	}
	
	def close() : Unit = {
	  _datastores map { _.close() }
	}
	

	override def toString() : String = {
		val sb = new StringBuilder()
		sb ++= "DataWarehouse[\n"
		_datastores.foreach( ds => {
			sb ++= "\t"
			sb ++= ds.toString
			sb ++= "\n"
			ds.tables.foreach( t => {				
				sb ++= "\t\t"
				sb ++= t.toString
				sb ++= "\n"	
				})
			})
		sb ++= "]\n"
		sb.toString
	}

	def test() : Boolean =  {
 		_datastores.foreach({ ds =>
 			ds.test()
 			})
 		true
 	}
}
