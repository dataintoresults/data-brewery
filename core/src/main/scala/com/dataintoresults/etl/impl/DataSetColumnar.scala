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

import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSet

class DataSetColumnar(_columns: Seq[Column], _columnsData: Seq[Seq[Any]]) extends DataSet {
	/*
	 * Trait DataSet.
	 */
  def columns : Seq[Column] = _columns
  
  def rows : Seq[Seq[Any]] = {
    if(_columnsData.length == 0)
      Seq(Seq())
    else {
      val nbRows = _columnsData(0).length
      
      for(i <- 0 until nbRows) yield _columnsData map {c => c(i) }
    }
  }
  
  /*
   * Other functions.
   */
  
	
	/**
	 * Is the dataset empty of data (regardless of the existence of columns).
	 */
	def isEmpty : Boolean = { _columnsData.head == Nil }	
}