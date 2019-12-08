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

import scala.reflect.runtime.universe._
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.dataintoresults.etl.core.{Column, EtlElement, EtlParameter, Table}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.util.XmlHelper._
import java.time.ZoneOffset

class ColumnBasic extends EtlElement("column") with Column {
	final val defaultTemporalFormat = "yyyy-MM-dd HH:mm:ss" 
	final val defaultMissingValue = "" 

  private val _name = EtlParameter[String](nodeAttribute = "name")
  def name: String = _name.value()
  
  private val _colType = EtlParameter[String](nodeAttribute = "type")
  def colType: String = _colType.value()
  
  private val _temporalFormat = EtlParameter[String](nodeAttribute = "temporalFormat", defaultValue=defaultTemporalFormat)
  def temporalFormat: String = _temporalFormat.value()

  private val _missingValue = EtlParameter[String](nodeAttribute = "missingValue", defaultValue=defaultMissingValue)
  def missingValue: String = _missingValue.value()

    
	def basicType : Column.BasicType = {	
		if(colType.equals("variant"))	Column.VARIANT;
		else if(colType.startsWith("boolean")) Column.BOOLEAN;
		else if(colType.equals("int"))  Column.INT;
		else if(colType.equals("bigint")) Column.BIGINT;
		else if(colType.startsWith("varchar")) Column.TEXT;
		else if(colType.startsWith("string")) Column.TEXT;
		else if(colType.startsWith("text")) Column.TEXT;
		else if(colType.startsWith("bigtext")) Column.BIGTEXT;
		else if(colType.startsWith("numeric")) Column.NUMERIC;
		else if(colType.equals("date")) Column.DATE;
		else if(colType.equals("datetime")) Column.DATETIME;
		else if(colType.equals("timestamp")) Column.DATETIME;
		else if(colType.equals("ident")) Column.INT;
		else if(colType.equals("bigident")) Column.BIGINT;
		else if(colType.equals("double")) Column.NUMERIC;
		else if(colType.equals("validityStartTimestamp")) Column.DATETIME;
		else if(colType.equals("validityEndTimestamp")) Column.DATETIME;
		else if(colType.equals("creationTimestamp")) Column.DATETIME;
		else if(colType.equals("updateTimestamp")) Column.DATETIME;
		else if(colType.equals("isDeleted")) Column.BOOLEAN;
		else if(colType.equals("lazy")) Column.LAZY;
		else
			throw new RuntimeException("Column "+name+" unknown type : " +colType);
	}

  
  
	def this(name: String, colType: String) = {
	  this()
    set(name, colType)
  }
		
	def this(colXml: scala.xml.Node) = {
	  this()
    parse(colXml)
  }	
	
	def set(name: String, colType: String) = {
    _name.value(name)
    _colType.value(colType)
		_temporalFormat.value(defaultTemporalFormat)
		_missingValue.value(defaultMissingValue)
  }

 	override def toString = s"Col[${name},${colType}]"

	def compareTo(o: Column) = {
		name.compareTo(o.name);
	}
	/*
  def fromXml(colXml: scala.xml.Node): Unit = {    
    _name = colXml \@? "name" getOrElse 
      (throw new RuntimeException("The column need to have a name attribute"))
    _colType = colXml \@? "type" getOrElse 
      (throw new RuntimeException("The column ${_name} need to have a type attribute"))
    _temporalFormat = colXml \@? "temporalFormat" getOrElse null
    _missingValue = colXml \@? "missingValue" getOrElse null
  }
  
  
  def toXml(): scala.xml.Node = {
    <column name={name} type={colType} temporalFormat={_temporalFormat} missingValue={_missingValue}/>
  }*/
  
  
  def fromString(cell: String): Any = {
    if(cell == null || cell == missingValue)
	    null
	  else
	    basicType match {
  	    case Column.DATETIME => 
  	      val format = DateTimeFormatter.ofPattern(temporalFormat)
  	      LocalDateTime.parse(cell, format)
  	    case Column.DATE => 
  	      val format = DateTimeFormatter.ofPattern(temporalFormat)
  	      LocalDate.parse(cell, format)
    		case Column.BIGINT => cell.toLong
    		case Column.INT => cell.toInt
    		case Column.BOOLEAN => cell.toBoolean
    		case Column.NUMERIC => new java.math.BigDecimal(cell) 
  	    case _ => cell
  	  }
  }
  
  def toString(a: Any): String = {
    if(a == null)
      missingValue
	  else
	    basicType match {
  	    case Column.DATE => 
  	      val format = DateTimeFormatter.ofPattern(temporalFormat)
  	      a.asInstanceOf[LocalDate].format(format)
  	    case Column.DATETIME => 
  	      val format = DateTimeFormatter.ofPattern(temporalFormat)
  	      a.asInstanceOf[LocalDateTime].format(format)
  	    case _ => a.toString
  	  }
  }
}

object ColumnBasic {
  def columnTypeFromScalaType[T: TypeTag]: String = {
    typeOf[T] match { 
      case t if t =:= typeOf[String] => "string"
      case t if t =:= typeOf[Long] => "bigint"
      case t if t <:< typeOf[scala.runtime.RichLong] => "bigint"
      case t if t <:< typeOf[Int] => "int"
      case t if t <:< typeOf[scala.runtime.RichInt] => "int"
      case t if t =:= typeOf[Double] => "double"
      case t if t <:< typeOf[scala.runtime.RichDouble] => "double"
      case t if t =:= typeOf[java.math.BigDecimal] => "numeric"
      case t if t =:= typeOf[LocalDateTime] => "datetime"
      case t if t =:= typeOf[LocalDate] => "date"
      case _ => "variant"
    }
  }
}
