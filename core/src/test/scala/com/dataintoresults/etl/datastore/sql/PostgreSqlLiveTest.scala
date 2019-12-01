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


/**
 * Live test of PostgreSQL backend.
 * 
 * In order not to be canceled, you need to provide configuration to a
 * testing PostgreSQL instance in a configutation file :
 * secret/postgresql/postgresql.conf
 * 
 * dw.datastore.test_pg {	
 *	host : ""
 * 	database : ""
 * 	user : ""
 * 	password : ""
 * }
 * 
 */
@Slow
class PostgreSqlLiveTest extends FunSuite {
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

		val expectedContent = "a, b\n99, toto\n2, tata\n"
		
		Try {
			store.execute("drop schema if exists test_schema cascade")
			store.execute("create schema test_schema")
			store.execute("drop table if exists test_schema.test_z")
			store.execute("create table test_schema.test_z as select 99 as a, 'toto' as b union all select 2, 'tata'")
		}.recover { 
			case _ => cancel(s"Couldn't setup the database for the test")
		}

		val table = store.table("test_z")
		assert(table.isDefined) withClue ", table 'test_z' not found"

		assertResult(expectedContent) {
			EtlHelper.printDataSource(table.get.read())
		} withClue ", returned data wasn't what was expected"
  }

  test("Live test of postgresql - auto discovery") { 
		val store = initStore()

		val expectedContent = "a, b\n2, toto\n2, tata\n"
		
		Try {
			store.execute("drop schema if exists test_auto cascade")
			store.execute("create schema test_auto")
			store.execute("create table test_auto.test_b as select 2 as a, 'toto' as b union all select 2, 'tata'")
			store.execute("create table test_auto.test_c as select 3 as a, 'toto' as b union all select 2, 'tata'")
		}.recover { 
			case _ => cancel(s"Couldn't setup the database for the test")
		}

		val table = store.table("test_b")
		assert(table.isDefined) withClue ", table 'test_auto.test_b' not found"

		assertResult(expectedContent) {
			EtlHelper.printDataSource(table.get.read())
		} withClue ", returned data wasn't what was expected"
	}
	
	
  test("Live test of postgresql - check date types") {
		val store = initStore()
		val ds = store.createDataSource("select '2019-11-01'::date as dt, '2019-11-01 00:58:25.314159'::timestamp as ts")
		val row = ds.next()
		val dtAny = row(0)
		val tsAny = row(1)
		assert(dtAny.isInstanceOf[java.time.LocalDate]) withClue "'2019-11-01'::date should be of time LocalDate"
		assert(tsAny.isInstanceOf[java.time.LocalDateTime]) withClue "'2019-11-01 00:58:25.314159'::timestamp should be of time LocalDateTime"
		val dt = dtAny.asInstanceOf[java.time.LocalDate]
		val ts = tsAny.asInstanceOf[java.time.LocalDateTime]
		assertResult(dt.getMonth())(java.time.Month.NOVEMBER) withClue "'2019-11-01'::date should be of month november"
		assertResult(ts.get(java.time.temporal.ChronoField.MICRO_OF_SECOND))(314159
			) withClue "'2019-11-01 00:58:25.314159'::timestamp should have microsecond of 314159."
	}


}