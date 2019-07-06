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

package com.dataintoresults.etl.explorer

import play.api.libs.json.JsObject
import play.api.libs.json.Json

import com.dataintoresults.etl.core.Module
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.ReplicateDataStore



case class DataStoreExplorer(val name: String, val schemas: Seq[SchemaExplorer]) { 
  def toXml() : scala.xml.Node = 
    <schema name={name}>
			{ schemas map { _.toXml() } } 
		</schema>
			
  def toJson() : JsObject = {
    Json.obj("name" -> name, "schemas" -> (schemas map (s => s.toJson())))
  }
}