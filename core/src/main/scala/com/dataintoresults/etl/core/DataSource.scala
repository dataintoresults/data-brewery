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

package com.dataintoresults.etl.core

/**
 * Represents a source of data.
 */
trait DataSource {  
	/**
	 * Returns the structure of the underlying data.
	 */
	def structure : Seq[Column]
	
	/**
	 * Does the data source still have data to be read by next?
	 */
	def hasNext() : Boolean

	/**
	 * Returns the next row in the stream of data.
	 * Throws an exception if there is no data to be read.
	 * Use hasNext to access this method safely.
	 */
	def next() : Seq[Any]
	
	/**
	 * Close this data source and release eventual resources taken.
	 * It is no longer possuble to read data after this call.
	 */
	def close() : Unit
	
	/**
	 * Apply a lambda to every rows in this data source.
	 * It closes the data source at the end.
	 */
	def foreach[B](f: Seq[Any] => B) : Unit = {
	  while(hasNext())
	    f(next())
		close()
	}
}