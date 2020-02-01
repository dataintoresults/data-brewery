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

package com.dataintoresults.etl.datastore.sql

import java.io.File


import scala.xml._
import scala.util.Try

import com.typesafe.config.ConfigFactory

import org.scalatest.FunSuite 
import org.scalatest.tags.Slow
import org.scalatest.AppendedClues._

import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.impl.EtlImpl

import com.dataintoresults.util.Using._


/**
 * Live test of Oracle backend.
 * 
 * In order not to be canceled, you need to provide configuration to a
 * testing Oracle instance in a configutation file :
 * secret/oracle/oracle.conf
 * 
 * dw.datastore.test_oracle {  
 *  host : ""
 *   database : ""
 *   user : ""
 *   password : ""
 * }
 * 
 */
@Slow
class OracleLiveTest extends FunSuite {

  def init(): EtlImpl  = {
    val xml = 
      <datawarehouse>
        <datastore name="test_oracle" type="oracle">
        </datastore>

        <module name="test_overwrite" datastore="test_oracle"> 
          <table name="input">
            <source type="query">
              SELECT floor(DBMS_RANDOM.VALUE()*3) AS "nb" from dual
            </source>
          </table>
          <table name="overwrite" strategy="overwrite" businessKeys="nb">
            <source type="module" module="test_overwrite" table="input"/>
          </table>
        </module>
      </datawarehouse>

    val configPath = "secret/oracle/oracle.conf"

    val configFile = new File(configPath)
  
    if(!configFile.exists)
      cancel(s"you need a configuration file $configPath for this test") 
  
    val config = ConfigFactory.parseFile(configFile)  

    val etl = new EtlImpl(config)
    etl.load(xml)

    etl
  }

  def initStore()  = {
    val xml = 
      <datastore name="test_oracle" type="oracle">
        <table name="TEST_A"/>
        <table schema="test_schema" name="test_z"/>
        <autoDiscovery schema="test_auto"/>
      </datastore>

    val configPath = "secret/oracle/oracle.conf"

    val configFile = new File(configPath)

    if(!configFile.exists)
      cancel(s"you need a configuration file $configPath for this test") 

    val config = ConfigFactory.parseFile(configFile)  

    Seq("host", "database", "user", "password").foreach { confKey =>
      if(!config.hasPath(s"dw.datastore.test_oracle.$confKey"))
        cancel(s"Test need config value dw.datastore.test_oracle.$confKey to be set")
    }

    val store = OracleStore.fromXml(xml, config)

    store
  }


  test("Live test of Oracle - read a table from current schema") {
    val store = initStore()

    val expectedContent = "A, B\n1, toto\n2, tata\n"
    
    Try {
      // Try safety because there is no if exists in Oracle
      Try(store.execute("drop table TEST_A"))
      store.execute("create table TEST_A as select 1 as A, 'toto' as B from dual union all select 2, 'tata' from dual")
    }.recover { 
      case _ => cancel(s"Couldn't setup the database for the test")
    }

    val table = store.table("TEST_A")
    assert(table.isDefined) withClue ", table 'TEST_A' not found"

    assertResult(expectedContent) {
      EtlHelper.printDataSource(table.get.read())
    } withClue ", returned data wasn't what was expected"
  }

  test("Live test of Oracle - read a table from specified schema") {
    val store = initStore()

    val expectedContent = "a, b\n99, toto\n2, tata\n"
    
    Try {
      // Try safety because there is no if exists in Oracle
      Try(store.execute("""drop user "test_schema" cascade"""))
      store.execute("""create user "test_schema" default tablespace USERS""");
      store.execute("""alter user "test_schema" quota unlimited on USERS""")
      // Try safety because there is no if exists in Oracle
      Try(store.execute("""drop table "test_schema"."test_z""""))
      store.execute("""create table "test_schema"."test_z" as 
        select 99 as "a", 'toto' as "b" from dual union all select 2, 'tata' from dual""")
    }.recover { 
      case _ => cancel(s"Couldn't setup the database for the test")
    }

    val table = store.table("test_z")
    assert(table.isDefined) withClue ", table 'test_z' not found"

    assertResult(expectedContent) {
      EtlHelper.printDataSource(table.get.read())
    } withClue ", returned data wasn't what was expected"
  }

  test("Live test of Oracle - auto discovery") { 
    val store = initStore()

    val expectedContent = "A, B\n2, toto\n2, tata\n"
    
    Try {
      Try(store.execute("""drop user "test_auto" cascade"""))
      store.execute("""create user "test_auto" default tablespace USERS""");
      store.execute("""alter user "test_auto" quota unlimited on USERS""")
      store.execute("""create table "test_auto"."test_b" as 
        select 2 as a, 'toto' as b from dual union all select 2, 'tata' from dual """)
      store.execute("""create table "test_auto"."test_c" as 
        select 3 as a, 'toto' as b from dual union all select 2, 'tata' from dual """)
    }.recover { 
      case _ => cancel(s"Couldn't setup the database for the test")
    }

    val table = store.table("test_b")
    assert(table.isDefined) withClue ", table 'test_auto.test_b' not found"

    assertResult(expectedContent) {
      EtlHelper.printDataSource(table.get.read())
    } withClue ", returned data wasn't what was expected"
  }
  
  
  test("Live test of Oracle - check date types") {
    val store = initStore()
    val ds = store.createDataSource("""select 
      DATE'2019-11-01' as dt, 
      to_timestamp('2019-11-01 04:58:25.314159', 'YYYY-MM-DD HH24:MI:SS.FF') as ts,
      cast(6 as number(9)) as int_value_2,
      cast(3.1415 as binary_float) as double_val,
      13 as decimal_value,
      cast(6 as number(18)) as bigint_value
      from dual""")
      
    val row = ds.next()    
    
    val dtAny = row(0)
    val tsAny = row(1)
    val intAny = row(2)
    val doubleAny = row(3)
    val decimalAny = row(4)
    val bigintAny = row(5)

    // DATE type in Oracle have time.
    assert(dtAny.isInstanceOf[java.time.LocalDateTime]) withClue "DATE'2019-11-01' should be of time LocalDateTime"
    assert(tsAny.isInstanceOf[java.time.LocalDateTime]) withClue "to_timestamp('2019-11-01 04:58:25.314159', 'YYYY-MM-DD HH24:MI:SS.FF') should be of time LocalDateTime"
    assert(intAny.isInstanceOf[java.lang.Integer]) withClue s"cast(6 as number(9)) should be of of type int it is ${intAny.getClass}"
    assert(doubleAny.isInstanceOf[java.lang.Double]) withClue s"cast(3.1415 as binary_double) should be of of type double it is ${doubleAny.getClass}"
    assert(decimalAny.isInstanceOf[java.math.BigDecimal]) withClue s"13 should be of of type decimal it is ${decimalAny.getClass}"
    assert(bigintAny.isInstanceOf[java.lang.Long]) withClue s"cast(6 as number(18)) should be of of type bigint it is ${bigintAny.getClass}"

    val dt = dtAny.asInstanceOf[java.time.LocalDateTime]
    val ts = tsAny.asInstanceOf[java.time.LocalDateTime]
    assertResult(dt.getMonth())(java.time.Month.NOVEMBER) withClue "'2019-11-01'::date should be of month november"
    assertResult(ts.get(java.time.temporal.ChronoField.MICRO_OF_SECOND))(314159
      ) withClue "'2019-11-01 04:58:25.314159'::timestamp should have microsecond of 314159."
      assertResult(ts.get(java.time.temporal.ChronoField.HOUR_OF_DAY))(4
        ) withClue "ensure no timzone issue (all should be UTC)"
  }

  test("Live test of Oracle - overwrite") {
    using(init()) { etl =>
      // First run to create the table
      etl.runModule("test_overwrite")

      // second run update the table
      etl.runModule("test_overwrite")

      // Throw a lot a refresh (enough to generate 0, 1 and 2)
      1 until 20 foreach { _ =>
        etl.runModule("test_overwrite")
      }

      assertResult(3)(etl.previewTableFromModule("test_overwrite", "overwrite").size)
        .withClue("The number of rows should not be greater that 3")
    }
  }

}