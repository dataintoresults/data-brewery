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
import org.scalatest.AppendedClues._
import scala.util.Success

class FilterFormulaTest extends FunSuite {
  
  test("Simple values should be parsed") {
    assertResult(ValueInt(123))(Value.parse("123")) withClue ", issue with simple integer"
    assertResult(ValueInt(123))(Value.parse(" 123 ")) withClue ", issue with simple integer with spaces"
    assertResult(ValueInt(0))(Value.parse("0")) withClue ", issue with integer 0"
    assertResult(ValueDouble(15.4))(Value.parse("15.4")) withClue ", issue with doubles"
    assertResult(ValueString("bob"))(Value.parse("\"bob\"")) withClue ", issue with string with double quotes"
    assertResult(ValueIdentifier("bob"))(Value.parse("bob")) withClue ", issue with string with identifiers"
  }
  
  test("Filters should be parsed") {
    assertResult(FilterSup(ValueIdentifier("duration"), ValueInt(0)))(FilterOp.parse("duration > 0")) withClue ", issue with filter duration > 0"
    assertResult(FilterEq(ValueIdentifier("user_id"), ValueInt(0)))(FilterOp.parse("user_id = 0")) withClue ", issue with user_id = 0"
  }
  
  
}