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

import java.io.StringReader
import java.time.{LocalDate, LocalDateTime}
import scala.math.BigDecimal

import org.scalatest.FunSuite
import org.scalatest.Assertions._


class ColumnBasicTest extends FunSuite {
  
  test("Column need to parse attributes") {
    val col = new ColumnBasic(<column name="a" type="datetime" temporalFormat="MM-dd/yyyy" missingValue="toto"/>)
    assertResult("a")(col.name)
    assertResult("datetime")(col.colType)
    assertResult("MM-dd/yyyy")(col.temporalFormat)
    assertResult("toto")(col.missingValue)
  }
  
  test("Column need to convert date correctly") {
    val col = new ColumnBasic(<column name="a" type="date" temporalFormat="MM-dd/yyyy" missingValue="toto"/>)
    val dt = LocalDate.of(2018, 1, 2)
    val dtString = "01-02/2018"
        
    assertResult(dtString, "Conversion of datetime to string")(col.toString(dt))
    assertResult(dt, "Conversion of string to datetime")(col.fromString(dtString))
    assertResult(dt.getClass, "Type for a date is java.time.LocalDate")(col.fromString(dtString).getClass)
    assertResult(null, "Conversion of missing value to datetime (missing value)")(col.fromString("toto"))
    assertResult(null, "Conversion of missing value to datetime (null value)")(col.fromString(null))
    assertResult("toto", "Conversion of missing value to string")(col.toString(null))
  }
  
  test("Column need to convert datetime correctly") {
    val col = new ColumnBasic(<column name="a" type="datetime" temporalFormat="MM-dd/yyyy HH" missingValue="toto"/>)
    val dt = LocalDateTime.of(2018, 1, 2, 16, 0, 0)
    val dtString = "01-02/2018 16"
        
    assertResult(dtString, "Conversion of datetime to string")(col.toString(dt))
    assertResult(dt, "Conversion of string to datetime")(col.fromString(dtString))
    assertResult(dt.getClass, "Type for a date is java.time.LocalDateTime")(col.fromString(dtString).getClass)
    assertResult(null, "Conversion of missing value to datetime (missing value)")(col.fromString("toto"))
    assertResult(null, "Conversion of missing value to datetime (null value)")(col.fromString(null))
    assertResult("toto", "Conversion of missing value to string")(col.toString(null))
  }
  
  
  test("Column need to convert bigint correctly") {
    val col = new ColumnBasic(<column name="a" type="bigint" missingValue="toto"/>)
    val long = 6000000000L
    val longString = "6000000000"
        
    assertResult(longString, "Conversion of bigint to string")(col.toString(long))
    assertResult(long, "Conversion of string to bigint")(col.fromString(longString))
    assertResult(classOf[java.lang.Long], "Type for a bigint is Long")(col.fromString(longString).getClass)
    assertResult(null, "Conversion of missing value to bigint (missing value)")(col.fromString("toto"))
    assertResult(null, "Conversion of missing value to bigint (null value)")(col.fromString(null))
    assertResult("toto", "Conversion of missing value to bigint")(col.toString(null))
  }
  
  test("Column need to handle numeric correctly") {
    val col = new ColumnBasic(<column name="a" type="numeric" missingValue="toto"/>)
    val numeric = (BigDecimal("0.3") - BigDecimal("0.2")).bigDecimal
    val numericString = "0.1"
    // would be 0.999999 in double and 0.1000001 in float
        
    assertResult(numericString, "Conversion of bigint to string")(col.toString(numeric))
    assertResult(numeric, "Conversion of string to bigint")(col.fromString(numericString))
    assertResult(numeric.getClass, "Type for a numeric is java.math.BigDecimal")(col.fromString(numericString).getClass)
    assertResult(null, "Conversion of missing value to bigint (missing value)")(col.fromString("toto"))
    assertResult(null, "Conversion of missing value to bigint (null value)")(col.fromString(null))
    assertResult("toto", "Conversion of missing value to bigint")(col.toString(null))
  }
  
}