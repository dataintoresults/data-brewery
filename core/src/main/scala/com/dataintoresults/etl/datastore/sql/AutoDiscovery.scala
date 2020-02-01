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

package com.dataintoresults.etl.datastore.sql

import com.dataintoresults.etl.core.{EtlElement, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

/**
 * Contains a schema to be automatically parsed and which table will be 
 * added to the parent SQL datastore.
 */
class AutoDiscovery extends EtlElement(AutoDiscovery.label) {  
  private val _schema = EtlParameter[String](nodeAttribute="schema")
  def schema = _schema.value
}

object AutoDiscovery {
  def label = "autoDiscovery"
}
