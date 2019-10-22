/*******************************************************************************
 *
 * Copyright (C) 2019 by Obsidian SAS : https://dataintoresults.com/
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

import scala.xml.XML

import java.io.StringReader
import java.time.{LocalDate, LocalDateTime}
import java.nio.file.{Files, Paths}
import java.nio.charset.Charset
import scala.math.BigDecimal

import org.apache.commons.io.FileUtils

import org.scalatest.FunSuite
import org.scalatest.Assertions._
import org.scalatest.AppendedClues._
import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.typesafe.config.ConfigFactory


class ParameterParserTest extends FunSuite {

  test("ParameterParser parsing tests") {
		val confContent = """
		module.var="test"
		"""
		val conf = ConfigFactory.parseString(confContent)
		assertResult("value")(ParameterParser.parse("value", conf)) withClue "Parsing a simple string parameter (shouldn't change anything)"
		assertResult("test")(ParameterParser.parse("${conf.module.var}", conf)) withClue "Parsing a configuration call"
	}
}