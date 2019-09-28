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
import fastparse._
import scala.util.{Try, Success, Failure}
import play.api.libs.json.{Json, JsValue}

sealed trait Value
sealed trait ValueLiteral extends Value {
	def toJson: JsValue
}
case class ValueInt(value: Int) extends ValueLiteral {
	def toJson = Json.toJson(value)
	override def toString = value.toString
}
case class ValueDouble(value: Double) extends ValueLiteral {
	def toJson = Json.toJson(value)
	override def toString = value.toString
}
case class ValueString(value: String) extends ValueLiteral {
	def toJson = Json.toJson(value)
	override def toString = "\""+value+"\""
}
case class ValueIdentifier(value: String) extends Value {
	def toJson = Json.toJson(value)
	override def toString = value
}

class ParserException(parserFailure: Parsed.Failure) extends RuntimeException(parserFailure.msg)

object Value {
	import ScalaWhitespace._
	def stringChars(c: Char) = c != '\"' && c != '\\' && c != '\''
	def idChars[_: P]        = CharsWhileIn("a-zA-Z0-9_")
	def strChars[_: P]       = CharsWhile(stringChars)
	def space[_: P]          = CharsWhileIn(" \r\t\n", 0)
	def digits[_: P]         = CharsWhileIn("0-9")
	def exponent[_: P]       = CharIn("eE") ~ CharIn("+\\-").? ~ digits
	def fractional[_: P]     = "." ~ digits
	def integral[_: P]       = "0" | CharIn("1-9") ~ digits.?
	def valueDouble[_: P]    = (CharIn("+\\-").? ~ integral ~ fractional ~ exponent.?).!.map(s => ValueDouble(s.toDouble))
	def valueInt[_: P]       = (CharIn("+\\-").? ~ integral).!.map(s => ValueInt(s.toInt))
	def valueString2[_: P]   = ( "\"" ~ strChars.rep.! ~ "\"").map(ValueString)
	def valueString1[_: P]   = ( "'" ~ strChars.rep.! ~ "'").map(ValueString)
	def valueString[_: P]    = (valueString2 | valueString1)
	def valueId[_: P]        = idChars.!.map(ValueIdentifier)
	def valueLiteral[_: P]  = (valueDouble | valueInt | valueString)
	def value[_: P]          = (valueLiteral | valueId)
	def valueOnly[_: P]      = space ~ value ~ space ~ End

	def parse(formula: String): Value = {
		fastparse.parse(formula, valueOnly(_)) match {
			case Parsed.Success(v, _) => v
			case f : Parsed.Failure => throw new ParserException(f)
		}
	}
}

sealed trait FilterOp {
	def left: ValueIdentifier
	def right: ValueLiteral
}
case class FilterEq(left: ValueIdentifier, right: ValueLiteral) extends FilterOp
case class FilterNeq(left: ValueIdentifier, right: ValueLiteral) extends FilterOp
case class FilterInf(left: ValueIdentifier, right: ValueLiteral) extends FilterOp
case class FilterSup(left: ValueIdentifier, right: ValueLiteral) extends FilterOp
case class FilterInfEq(left: ValueIdentifier, right: ValueLiteral) extends FilterOp
case class FilterSupEq(left: ValueIdentifier, right: ValueLiteral) extends FilterOp

object FilterOp {
	import ScalaWhitespace._
	def literal[_: P]     = Value.valueLiteral
	def space[_: P]       = Value.space
	def identifier[_: P]  = Value.valueId
	def filterEq[_: P]    = (identifier ~ "=" ~ literal).map { case (id, value) => FilterEq(id, value) }
	def filterNeq[_: P]   = (identifier ~ ( "!=" | "<>") ~ literal).map { case (id, value) => FilterNeq(id, value) }
	def filterInf[_: P]   = (identifier ~ "<" ~ literal).map { case (id, value) => FilterInf(id, value) }
	def filterSup[_: P]   = (identifier ~ ">" ~ literal).map { case (id, value) => FilterSup(id, value) }
	def filterInfEq[_: P] = (identifier ~ "<=" ~ literal).map { case (id, value) => FilterInfEq(id, value) }
	def filterSupEq[_: P] = (identifier ~ ">=" ~ literal).map { case (id, value) => FilterSupEq(id, value) }
	def filter[_: P]      = (filterEq | filterNeq | filterInfEq | filterSupEq | filterInf | filterSup)
	def filterOnly[_: P]  = space ~ filter ~ space ~ End

	def parse(formula: String): FilterOp = {
		fastparse.parse(formula, filterOnly(_)) match {
			case Parsed.Success(v, _) => v
			case f : Parsed.Failure => throw new ParserException(f)
		}
	}
}

class FilterFormula extends EtlElement("filter") {
  private val _formula = EtlParameter[String](nodeAttribute = "formula", cdata = true)
	def formula: String = _formula.value()
	
	def filter = FilterOp.parse(formula)  
	override def toString = s"Filter[${formula}]"

	def compareTo(o: FilterFormula) = {
		formula.compareTo(o.formula);
	}
}