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
import scala.util.{Try, Success, Failure}
import com.typesafe.config.ConfigFactory

import org.apache.commons.io.FileUtils

import org.scalatest.FunSuite
import org.scalatest.Assertions._
import org.scalatest.AppendedClues._
import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.core.Task
import com.dataintoresults.etl.datastore.sql.SqlStore


class EtlImplShellTest extends FunSuite {
	val dw1 = 
		<datawarehouse>
			<process name="test">
				<task shellCommand="ls">
					<parameter>-alh</parameter>
					<parameter value="."/>
					<parameter value="${conf.module.variable}"/>
				</task>
			</process>
		</datawarehouse>
	
	val configContent = """module.variable = "." """

  test("Etl need to process shell tasks") {
		val etl = new EtlImpl(ConfigFactory.parseString(configContent).withFallback(ConfigFactory.load()))
		etl.load(dw1)

		val processOpt = Try(etl.findProcess("test"))
		
		processOpt match {
			case Failure(_) => fail("Didn't find test process ...")
			case Success(process) => { 
				assertResult(1)(process.tasks.size) withClue "There should be one and only one task in this settings"
				val task = process.tasks.head
				assertResult(Task.SHELL)(task.taskType) withClue "The task should be of type SHELL"
				assertResult("ls")(task.shellCommand) withClue "Test the task command"
				assertResult(3)(task.shellParameters.size) withClue "Test the task parameters count"
				assertResult("-alh")(task.shellParameters(0)) withClue "Test the task parameters count"
				assertResult(".")(task.shellParameters(1)) withClue "Test the task parameters count"
			}
		}
		
		// It will not work if parameter 3 is not 
		etl.runProcess("test")
	}
}