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
import com.dataintoresults.util.Using._
import com.dataintoresults.etl.impl.EtlImpl


/**
 * Live test of MySQL backend.
 * 
 * In order not to be canceled, you need to provide configuration to a
 * testing MySQL instance in a configutation file :
 * secret/mysql/mysql.conf
 * 
 * dw.datastore.test_mysql {	
 *	host : ""
 * 	database : ""
 * 	user : ""
 * 	password : ""
 * }
 * 
 */
@Slow
class MySqlLiveTest extends FunSuite {

	def initStore()  = {
		val xml = 
			<datastore name="test_mysql" type="mysql"> 
				<table name="test_a"/>
				<table schema="test_schema" name="test_z"/>
				<autoDiscovery schema="test.auto"/>  
			</datastore>

		val configPath = "secret/mysql/mysql.conf"

		val configFile = new File(configPath)

		if(!configFile.exists)
			cancel(s"you need a configuration file $configPath for this test") 
 
		val config = ConfigFactory.parseFile(configFile)  

		Seq("host", "database", "user", "password").foreach { confKey => 
			if(!config.hasPath(s"dw.datastore.test_mysql.$confKey"))
				cancel(s"Test need config value dw.datastore.test_mysql.$confKey to be set")
		}

		val store = MySqlStore.fromXml(xml, config)

		store.open()
		store
	}

	def initLowCredsStore()  = {
		val xml = 
			<datastore name="test_mysql_low_creds" type="mysql"> 
				<autoDiscovery schema="test_auto"/> 
			</datastore>

		val configPath = "secret/mysql/mysql.conf"

		val configFile = new File(configPath)

		if(!configFile.exists)
			cancel(s"you need a configuration file $configPath for this test") 

		val config = ConfigFactory.parseFile(configFile)  

		Seq("host", "database", "user", "password").foreach { confKey => 
			if(!config.hasPath(s"dw.datastore.test_mysql_low_creds.$confKey"))
				cancel(s"Test need config value dw.datastore.test_mysql_low_creds.$confKey to be set")
		}

		val store = MySqlStore.fromXml(xml, config)

		store.open()
		store
	}


  test("Live test of mysql - read a table from current schema") {
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

  test("Live test of mysql - read a table from specified schema") {
		val store = initStore()

		val expectedContent = "a, b\n99, toto\n2, tata\n"
		
		Try {
			store.execute("drop schema if exists test_schema")
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

  test("Live test of mysql - auto discovery") { 
		val store = initStore()

		val expectedContent = "a, b\n2, toto\n2, tata\n"
		
		Try {
			store.execute("drop schema if exists `test.auto`")
			store.execute("create schema `test.auto`")
			store.execute("create table `test.auto`.test_b as select 2 as a, 'toto' as b union all select 2, 'tata'")
			store.execute("create table `test.auto`.test_c as select 3 as a, 'toto' as b union all select 2, 'tata'")
		}.recover { 
			case _ => cancel(s"Couldn't setup the database for the test")
		}

		// Relaunch the discovery process to find new tables.
		store.discoverTables()

		val table = store.table("test_b")
		assert(table.isDefined) withClue ", table '`test.auto`.test_b' not found"

		assertResult(expectedContent) {
			EtlHelper.printDataSource(table.get.read())
		} withClue ", returned data wasn't what was expected"
	}
	
  test("Live test of mysql - auto discovery (low credentials version)") { 
		val adminStore = initStore()

		val expectedContent = "a, b\n2, toto\n2, tata\n"
		
		Try {
			adminStore.execute("drop schema if exists test_auto")
			adminStore.execute("create schema test_auto")
			adminStore.execute("create table test_auto.test_b as select 2 as a, 'toto' as b union all select 2, 'tata'")
			adminStore.execute("create table test_auto.test_c as select 3 as a, 'toto' as b union all select 2, 'tata'")
			adminStore.close()
		}.recover { 
			case _ => cancel(s"Couldn't setup the database for the test")
		}

		val store = initLowCredsStore()
		val table = store.table("test_b")
		assert(table.isDefined) withClue ", table 'test_auto.test_b' not found"
		assertResult(expectedContent) {
			EtlHelper.printDataSource(table.get.read())
		} withClue ", returned data wasn't what was expected"
  }

}