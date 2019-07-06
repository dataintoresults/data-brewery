/*******************************************************************************
 *
 * Copyright (C) 2018by Obsidian SAS : https://dataintoresults.com/
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

package com.dataintoresults.etl.datastore.odoo

import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.impl.ColumnBasic

import com.dataintoresults.etl.core.{EtlColumn, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

/**
 * Describe extended attributes of a Odoo column.
 */
class OdooColumn extends ColumnBasic  {  
  private val _odooField = EtlParameter[String](nodeAttribute="odooField")
  
  def odooField = _odooField.value

  def this(column: Column, odooField: String) = {
    this()
    set(column.name, column.colType, odooField)
  }
  
  def this(name: String, colType: String, odooField: String) = {
    this()
    set(name, colType, odooField)
  }
  
  def set(name: String, colType: String, odooField: String) = {
    super.set(name, colType)
    this._odooField.set(odooField)
  }
  
  
}