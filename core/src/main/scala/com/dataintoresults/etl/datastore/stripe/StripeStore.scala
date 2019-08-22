/*******************************************************************************
 *
 * Copyright (C) 2019 by Obsidian SAS : https://dataintoresults.com/
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

package com.dataintoresults.etl.datastore.stripe



import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.impl.DataSetImpl

import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class StripeStore extends EtlDatastore with DataStore {
 	private val _key = EtlParameter[String](nodeAttribute="key", configAttribute = "dw.datastore."+name+".key")

 	var _tables : Seq[StripeTable] = List[StripeTable]()

	def key = _key.value

 	def tables : Seq[Table] = _tables

 	override def toString = s"StripeStore[${key.take(4)+"*******"}]"
 	
 	
	def close() : Unit = {
	}
 	
	
	def createDataSource(stripeTable: StripeTable) : DataSource = ???
}

object StripeStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): DataStore = {
    val store = new StripeStore()
    store.parse(dsXml, config)
    store
  }
}