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
 * Live test of PostgreSQL backend.
 * 
 * In order not to be canceled, you need to provide configuration to a
 * testing PostgreSQL instance in a configutation file :
 * secret/postgresql/postgresql.conf
 * 
 * dw.datastore.test_pg {  
 *  host : ""
 *   database : ""
 *   user : ""
 *   password : ""
 * }
 * 
 */
@Slow
class PostgreSqlLiveTest extends FunSuite {

  def init(): EtlImpl  = {
    val xml = 
      <datawarehouse>
        <datastore name="test_pg" type="postgresql">
        </datastore>

        <module name="test_pg_overwrite" datastore="test_pg"> 
          <table name="input">
            <source type="query">
              SELECT floor(random()*3) AS nb
            </source>
          </table>
          <table name="overwrite" strategy="overwrite" businessKeys="nb">
            <source type="module" module="test_pg_overwrite" table="input"/>
          </table>
        </module>
      </datawarehouse>

    val configPath = "secret/postgresql/postgresql.conf"

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
      <datastore name="test_pg" type="postgresql">
        <table name="test_a"/>
        <table schema="test_schema" name="test_z"/>
        <autoDiscovery schema="test_auto"/>
      </datastore>

    val configPath = "secret/postgresql/postgresql.conf"

    val configFile = new File(configPath)

    if(!configFile.exists)
      cancel(s"you need a configuration file $configPath for this test") 

    val config = ConfigFactory.parseFile(configFile)  

    Seq("host", "database", "user", "password").foreach { confKey =>
      if(!config.hasPath(s"dw.datastore.test_pg.$confKey"))
        cancel(s"Test need config value dw.datastore.test_pg.$confKey to be set")
    }

    val store = PostgreSqlStore.fromXml(xml, config)

    store
  }


  test("Live test of postgresql - read a table from current schema") {
    val store = initStore()

    val expectedContent = "a, b\n1, toto\n2, tata\n"
    
    Try {
      store.execute("drop table if exists test_a")
      store.execute("create table test_a as select 1 as a, 'toto' as b union all select 2, 'tata'")
    }.recover { 
      case _ => cancel(s"Couldn't setup the database for the test")
    }

    val table = store.table("test_a")
    assert(table.isDefined) withClue ", table 'test_a' not found"

    assertResult(expectedContent) {
      EtlHelper.printDataSource(table.get.read())
    } withClue ", returned data wasn't what was expected"
  }

  test("Live test of postgresql - read a table from specified schema") {
    val store = initStore()

    Try {
      store.execute("drop schema if exists test_schema cascade")
      store.execute("create schema test_schema")
      store.execute("drop table if exists test_schema.test_z")
      store.execute("create table test_schema.test_z as select 99 as a, 'toto' as b union all select 2, 'tata'")
    }.recover { 
      case _ => cancel(s"Couldn't setup the database for the test")
    }
    val expectedContent = "a, b\n99, toto\n2, tata\n"
    

    val table = store.table("test_z")
    assert(table.isDefined) withClue ", table 'test_z' not found"

    assertResult(expectedContent) {
      EtlHelper.printDataSource(table.get.read())
    } withClue ", returned data wasn't what was expected"
  }

  test("Live test of postgresql - auto discovery") { 
    {
      val store = initStore()

      Try {
        store.execute("drop schema if exists test_auto cascade")
        store.execute("create schema test_auto")
        store.execute("create table test_auto.test_b as select 2 as a, 'toto' as b union all select 2, 'tata'")
        store.execute("create table test_auto.test_c as select 3 as a, 'toto' as b union all select 2, 'tata'")
      }.recover { 
        case _ => cancel(s"Couldn't setup the database for the test")
      }
    }

    val store = initStore()

    val expectedContent = "a, b\n2, toto\n2, tata\n"
    
    val table = store.table("test_b")
    assert(table.isDefined) withClue ", table 'test_auto.test_b' not found"

    assertResult(expectedContent) {
      EtlHelper.printDataSource(table.get.read())
    } withClue ", returned data wasn't what was expected"
  }
  
  
  test("Live test of postgresql - check date types (DataSource)") {
    val store = initStore()
    using(store.createDataSource("""select '2019-11-01'::date as dt, 
      '2019-11-01 04:58:25.314159'::timestamp as ts,
      6 as int_value,
      7::bigint as bigint_value,
      3.1415 as pi_value,
      3.1416::double precision as double_value,
      3.1417::float as float_value,
      'long text'::varchar(60000) as long_text_value,
      'short text'::varchar(255) as short_text_value,
      null::varchar(255) as null_varchar,
      null::int as null_int""")) { ds => 

      val row = ds.next()
      val dtAny = row(0)
      val tsAny = row(1)
      val intAny = row(2)
      val bigintAny = row(3)
      val numericAny = row(4)
      val doubleAny = row(5)
      val floatAny = row(6)
      val textAny = row(7)
      val bigtextAny = row(8)
      val nullVarcharAny = row(9)
      val nullIntAny = row(10)


      ds.close

      assert(dtAny.isInstanceOf[java.time.LocalDate]) withClue s"'2019-11-01'::date should be of type LocalDate but is ${dtAny.getClass}"
      assert(tsAny.isInstanceOf[java.time.LocalDateTime]) withClue s"'2019-11-01 04:58:25.314159'::timestamp should be of type LocalDateTime but is ${tsAny.getClass}"
      assert(intAny.isInstanceOf[java.lang.Integer]) withClue s"6 should be of type Integer but is ${intAny.getClass}"
      assert(bigintAny.isInstanceOf[java.lang.Long]) withClue s"7::bigint should be of type Long but is ${bigintAny.getClass}"
      assert(numericAny.isInstanceOf[java.math.BigDecimal]) withClue s"3.1415 should be of type BigDecimal but is ${numericAny.getClass}"
      assert(doubleAny.isInstanceOf[java.lang.Double]) withClue s"3.1416::double precision should be of type Double but is ${doubleAny.getClass}"
      assert(floatAny.isInstanceOf[java.lang.Double]) withClue s"3.1417::float should be of type LocalDateTime but is ${floatAny.getClass}"
      assert(textAny.isInstanceOf[String]) withClue s"'long text'::varchar(60000) should be of type String but is ${textAny.getClass}"
      assert(bigtextAny.isInstanceOf[String]) withClue s"'short text'::varchar(255) should be of type String but is ${bigtextAny.getClass}"
      assert(nullVarcharAny == null) withClue s"null::varchar(255) should be null"
      assert(nullIntAny == null) withClue s"null::int should be null"


      val dt = dtAny.asInstanceOf[java.time.LocalDate]
      val ts = tsAny.asInstanceOf[java.time.LocalDateTime]
      assertResult(dt.getMonth())(java.time.Month.NOVEMBER) withClue "'2019-11-01'::date should be of month november"
      assertResult(ts.get(java.time.temporal.ChronoField.MICRO_OF_SECOND))(314159
        ) withClue "'2019-11-01 04:58:25.314159'::timestamp should have microsecond of 314159."
        assertResult(ts.get(java.time.temporal.ChronoField.HOUR_OF_DAY))(4
          ) withClue "ensure no timzone issue (all should be UTC)"
    }
  }

  
  test("Live test of postgresql - check date types (DataSink)") {
    val store = initStore()
    using(store.createDataSource("""select '2019-11-01'::date as dt, 
      '2019-11-01 04:58:25.314159'::timestamp as ts,
      6 as int_value,
      7::bigint as bigint_value,
      3.1415 as pi_value,
      3.1416::double precision as double_value,
      3.1417::float as float_value,
      'long text'::varchar(60000) as long_text_value,
      'short text'::varchar(255) as short_text_value""")) { ds =>

    store.execute("drop schema if exists test_sink cascade")
    store.execute("create schema test_sink")

    store.createTable("test_sink", "sink", ds.structure)
    using(store.createDataSink("test_sink", "sink", ds.structure)) { sink =>
      sink.put(ds.next)
    }
  }


    
    using(store.createDataSource("""select * from test_sink.sink""")) { ds =>

      val row = ds.next()
      val dtAny = row(0)
      val tsAny = row(1)
      val intAny = row(2)
      val bigintAny = row(3)
      val numericAny = row(4)
      val doubleAny = row(5)
      val floatAny = row(6)
      val textAny = row(7)
      val bigtextAny = row(8)

      assert(dtAny.isInstanceOf[java.time.LocalDate]) withClue s"'2019-11-01'::date should be of type LocalDate but is ${dtAny.getClass}"
      assert(tsAny.isInstanceOf[java.time.LocalDateTime]) withClue s"'2019-11-01 04:58:25.314159'::timestamp should be of type LocalDateTime but is ${tsAny.getClass}"
      assert(intAny.isInstanceOf[java.lang.Integer]) withClue s"6 should be of type Integer but is ${intAny.getClass}"
      assert(bigintAny.isInstanceOf[java.lang.Long]) withClue s"7::bigint should be of type Long but is ${bigintAny.getClass}"
      assert(numericAny.isInstanceOf[java.math.BigDecimal]) withClue s"3.1415 should be of type BigDecimal but is ${numericAny.getClass}"
      assert(doubleAny.isInstanceOf[java.lang.Double]) withClue s"3.1416::double precision should be of type Double but is ${doubleAny.getClass}"
      assert(floatAny.isInstanceOf[java.lang.Double]) withClue s"3.1417::float should be of type LocalDateTime but is ${floatAny.getClass}"
      assert(textAny.isInstanceOf[String]) withClue s"'long text'::varchar(60000) should be of type String but is ${textAny.getClass}"
      assert(bigtextAny.isInstanceOf[String]) withClue s"'short text'::varchar(255) should be of type String but is ${bigtextAny.getClass}"


      val dt = dtAny.asInstanceOf[java.time.LocalDate]
      val ts = tsAny.asInstanceOf[java.time.LocalDateTime]
      assertResult(dt.getMonth())(java.time.Month.NOVEMBER) withClue "'2019-11-01'::date should be of month november"
      assertResult(ts.get(java.time.temporal.ChronoField.MICRO_OF_SECOND))(314159
        ) withClue "'2019-11-01 04:58:25.314159'::timestamp should have microsecond of 314159."
        assertResult(ts.get(java.time.temporal.ChronoField.HOUR_OF_DAY))(4
          ) withClue "ensure no timzone issue (all should be UTC)"
    }
  }

  test("Live test of Postgresql - overwrite") {
    using(init()) { etl =>
      // First run to create the table
      etl.runModule("test_pg_overwrite")

      // second run update the table
      etl.runModule("test_pg_overwrite")

      // Throw a lot a refresh (enough to generate 0, 1 and 2)
      1 until 20 foreach { _ =>
        etl.runModule("test_pg_overwrite")
      }

      assertResult(3)(etl.previewTableFromModule("test_pg_overwrite", "overwrite").size)
        .withClue("The number of rows should not be greater that 3")
    }
  }

}