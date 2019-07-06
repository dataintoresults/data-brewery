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
 * A table represents a place when you can read and/or write data
 * that conforms with a defined structure (set of columns).
 */
trait Table {
  /**
   * The name of the table.
   */
	def name : String;

  /**
   * Data structure of the table.
   */
  def columns : Seq[Column]
  
  /**
   * Check if it's possible to read from this table.
   */
  def canRead : Boolean 

  /**
   * Return a data source that iterate over the underlying data
   * of this table.
   * Raise an exception if this operation is not allowed by the backend.
   */
  def read() : DataSource

  /**
   * Check if it's possible to write to this table.
   */
  def canWrite : Boolean
  
  /**
   * Return a data source that iterate over the underlying data
   * of this table.
   * Raise an exception if this operation is not allowed by the backend.
   */
  def write() : DataSink
  
  def dropTableIfExists() : Table 
  def createTable() : Table 
  
  def test() : Boolean = true
  
  def toXml() : scala.xml.Elem
  
  def hasSource() : Boolean = false
  
  def source : Source = throw new RuntimeException("There is no source for this table")
}