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

object ExcelHelper {
	/*
	 * Returns a 0based index for a column name (A, B, C, ..., AA, AB, ...)
	 */
	def colToIndex(col: String): Int = { 
		col.toUpperCase.reverse.zipWithIndex
			.foldRight(-1 /* A will give +1 and it's 0-based */) { 
				case ((c, i), sum) => sum + (c - 'A' + 1)*Math.pow(26, i).toInt
	  }
	}
}
