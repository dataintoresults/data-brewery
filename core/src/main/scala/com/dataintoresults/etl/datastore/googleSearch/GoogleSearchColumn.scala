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

package com.dataintoresults.etl.datastore.googleSearch

import com.dataintoresults.etl.core.{Column, EtlColumn, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.ColumnBasic

class GoogleSearchColumn extends ColumnBasic  {
  val _gsName = EtlParameter[String](nodeAttribute = "gsName", defaultValue = Some(null))

  def gsName = if(_gsName.value == null || _gsName.value == "") name else _gsName.value


  def this(column: Column, gsName: String) = {
    this()
    set(column.name, column.colType, gsName)
  }

  def this(name: String, colType: String, gsName: String) = {
    this()
    set(name, colType, gsName)
  }

  def set(name: String, colType: String, gsName: String) = {
    super.set(name, colType)
    _gsName.value(gsName)
  }

}
