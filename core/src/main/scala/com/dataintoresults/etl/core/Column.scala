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
 * Represent a column of a data structure.
 *
 * Columns have types as well as basic types which are
 * types that behave similarly from a data perspective.
 */
trait Column extends Comparable[Column] {
      
	/**
	 * Returns the name of the column.
	 */
  def name: String
  
	/**
	 * Returns the detailed type of the column.
	 */
  def colType: String
  
	/**
	 * Returns the generic type of the column, generally 
	 * infered from colType.
	 */
	def basicType : Column.BasicType	
	
	/**
	 * Returns an XML representation of this column definition.
	 */
  def toXml() : scala.xml.Node
  
	/**
	 * Convert any string value to the underlying data representation of this colulmn.
	 */
  def fromString(s: String): Any
  
	/**
	 * Convert an underlying data representation to a string.
	 */
  def toString(a: Any): String
}

object Column {  
	sealed trait BasicType

	/**
	 * Variant types are used when the underlying type can change over type.
	 * Unexpected behavior for now.
	 */
	case object VARIANT extends BasicType

	/**
	 * Lazy types are placeholder when the real type is not yet known.
	 * Used for internal usage.
	 */
	case object LAZY extends BasicType

	/**
	 * Boolean types are either true or false (string representation 'true' and 'false')
	 * Know types : boolean
	 */
	case object BOOLEAN extends BasicType

	/**
   * Integer types are signed finites numbers between -2^31 and +2^31 (around 2 billions).
	 * Know types : int
	 */
	case object INT extends BasicType

	/**
   * Integer types are signed finites numbers between -2^63 and +2^63.
	 * Know types : bigint
	 */
	case object BIGINT extends BasicType
	
	/**
   * Text types that are expected to be shorts.
	 * The short definition is backend dependant, 
	 * but a safe bet is below 255 characters. 
	 * Know types : text, varchar (legacy)
	 */
	case object TEXT extends BasicType

	/**
   * Big Text types that are long chunks of text.
	 */
	case object BIGTEXT extends BasicType
	
	/**
   * Numeric types (non finites).
	 * Known types : float, double, decimal
	 */
	case object NUMERIC extends BasicType
	
	/**
   * Dates types (without intradays informations).
	 * Known types : date
	 */
	case object DATE extends BasicType

	/**
   * Dates types (with intradays informations).
	 * The timezone information is not expected, and by convention it is UTC.
	 * Known types : datetime, creationTimestamp, updateTimestamp
	 */
	case object DATETIME extends BasicType
}