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
 * Represent a place where we can emit rows conforming to the structure.
 * 
 * Any DataSink shoul dbe closed after use in order to unlock eventual resources.
 */
trait DataSink {  
	/**
	 * Return the row structure that this DataSink require.
	 * This can return null which mean the metadata is not know yet.
	 */ 
	def structure : Seq[Column]
	
	/**
	 * Before calling the put method, a provider of data can 
	 * call the setIncomingStruture method to.inform the row
	 * structure that will be used.
	 * An exception can be throwed if the structure is not
	 * compatible with the datasink structure
	 */
	def setIncomingStruture(incomingStructure: Seq[Column]): Unit = {
		// TODO : check structure and incomingStructure compatiility
	}

	/**
	 * Emit a row to this data sink.
	 */
	def put(row: Seq[Any]) : Unit
	
	/**
	 * Close the data sink. *put* can no longer be used after a close.
	 */
	def close() : Unit
}