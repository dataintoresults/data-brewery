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
import com.dataintoresults.etl.core.Etl
import com.dataintoresults.etl.core.ReplicateDataStore
import com.dataintoresults.etl.core.Table

class ReplicateDataStoreImpl(private val _etl: Etl, private var _datastore : DataStore, 
    private val _strategy: String = null) extends ReplicateDataStore {

  
  def strategy : String = if(_strategy != null) _strategy else "rebuild" 
  
  def dataStore : DataStore = _datastore
  
  def dataStore_  (ds: DataStore):Unit = _datastore = ds
 
  def tables : Seq[Table] = {
    _datastore.tables
  }
 
  def toXml() : scala.xml.Node = {
    <replicate datastore={dataStore.name} strategy={_strategy}/>
  }
}