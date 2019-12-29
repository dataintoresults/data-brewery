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
import scala.util.{Try}

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
import com.dataintoresults.util.Using._
import com.dataintoresults.etl.core.ProcessResult



class ProcessTest extends FunSuite  {
  val dw1 = 
    <datawarehouse>
      <datastore name="dw" type="h2"/>

      <!-- Module that should work -->
      <module name="ok" datastore="dw">
        <table name="ok">
          <source type="query">
            select 1 as a
          </source>
        </table>
      </module>

      <!-- Module that should FAIL -->
      <module name="nok" datastore="dw">
        <table name="nok">
          <source type="query">
            select 1/0
          </source>
        </table>
      </module>

      <process name="shouldSucceed">
        <task module="ok"/>
      </process>

      <process name="shouldFail">
        <task module="nok"/>
      </process>

      <process name="shouldWarn">
        <task module="nok" onError="warning"/>
        <task module="ok"/>
      </process>

      <process name="doubleWarn">
        <task module="nok" onError="warning"/>
        <task module="nok" onError="warning"/>
        <task module="ok"/>
      </process>
    </datawarehouse>

    
  test("Expect correct ProcessResult in simple cases") {
    using(new EtlImpl()) { implicit etl =>
      // Should not thow
      Try(etl.load(dw1)).recover { case e : Exception =>
        fail("Shouldn't throw an exception : " + e.getMessage)
      }

      Try(etl.runProcess("shouldSucceed")).fold(
        ex => fail("runModule shouldn't raise an exception when it suceed : " + ex.getMessage),
        v => assertResult(ProcessResult.Success)(v.status) withClue "Status should ne ProcessResult.Success when it works"
      )

      Try(etl.runProcess("shouldFail")).fold(
        ex => fail("runModule shouldn't raise an exception if it fail : " + ex.getMessage),
        v => {
          assert(v.status == ProcessResult.Error) withClue "Should be Error when it fail"
          assertResult(1)(v.errors.length) withClue "Error count should be 1 for shouldFail process"
        }
      )
    }
  }
    
  test("Expect correct ProcessResult in warn cases") {
    using(new EtlImpl()) { implicit etl =>
      // Should not thow
      Try(etl.load(dw1)).recover { case e : Exception =>
        fail("Shouldn't throw an exception : " + e.getMessage)
      }

      Try(etl.runProcess("shouldWarn")).fold(
        ex => fail("runModule shouldn't raise an exception if it fail : " + ex.getMessage),
        v => {
          assert(v.status == ProcessResult.Warning) withClue "Should be Warning because nok failed"
          assertResult(1)(v.warnings.length) withClue "Warning count should be 1 for shouldWarn process"
          assertResult(0)(v.errors.length) withClue "Error count should be 0 for shouldWarn process"
        }
      )
      
      Try(etl.runProcess("doubleWarn")).fold(
        ex => fail("runModule shouldn't raise an exception if it fail : " + ex.getMessage),
        v => {
          assert(v.status == ProcessResult.Warning) withClue "Should be Warning because nok failed"
          assertResult(2)(v.warnings.length) withClue "Warning count should be 2 for shouldWarn process"
          assertResult(0)(v.errors.length) withClue "Error count should be 0 for shouldWarn process"
        }
      )
    }
  }


}