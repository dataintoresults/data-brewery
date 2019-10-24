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

package com.dataintoresults.etl.datastore.flat


import com.typesafe.config.Config

import com.dataintoresults.etl.core.{DataStore, Table}
import com.dataintoresults.util.XmlHelper._
import com.dataintoresults.etl.impl.EtlImpl


import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter, Table}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.DataSetImpl
import com.dataintoresults.util.InHelper._

class FlatFileStore extends EtlDatastore with DataStore {  	
	private val _location = EtlParameter[String](nodeAttribute="location", configAttribute="dw.datastore."+name+".location", defaultValue=Some(null))

  def location: String = if(_location.value == null) "" else _location.value
  

	private val _tables = EtlChilds[CSVTable]()
	def tables : Seq[Table] = _tables
	 
	def close(): Unit = {}

}


object FlatFileStore {
  final val TYPE = "flat" 
  
	def fromXml(dsXml : scala.xml.Node, config: Config) : DataStore = {
		val store = new FlatFileStore()
		store.parse(dsXml, config)
		store
	}
}