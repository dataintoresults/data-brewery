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
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.Source
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink

case class TableExplorer(val name: String)  {      
  def toXml() : scala.xml.Node = 
    <table name={name}/>
  
  def toJson() : JsObject = {
    Json.obj("name" -> name)
  }
}