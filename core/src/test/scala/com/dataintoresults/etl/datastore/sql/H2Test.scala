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
import com.dataintoresults.util.Using.using


/**
 * H2 data store test.
 * 
 */
@Slow
class H2Test extends FunSuite {
	def init(): EtlImpl  = {
		val xml = 
			<datawarehouse>
				<datastore name="test_h2_mem" type="h2"> 
					<table name="test_a">
						<source type="query">
							select 1 as a
						</source>
					</table>
				</datastore>

				<datastore name="test_h2_fs" type="h2" host="file" database="~/h2dbtest">
					<table name="test_a">
						<source type="query">
							select 1 as a
						</source>
					</table>
				</datastore>

				<datastore name="test_h2_srv" type="h2" host="tcp://localhost/" database="./db1" user="sa" password="sa">
					<table name="test_a">
						<source type="query">
							select 1 as a
						</source>
					</table>
				</datastore>
			</datawarehouse>

		val etl = new EtlImpl()
		etl.load(xml)

		etl
	}


  test("Live test of H2 - in memory version") {
		using(init()) { etl =>
			val store = etl.dataWarehouse.datastores.find(_.name == "test_h2_mem")
				.getOrElse(fail("Issue to find the test_h2_mem datastore"))

			val table = store.table("test_a")
			assert(table.isDefined) withClue ", table 'test_a' not found"
 
			Try(etl.runDataStore("test_h2_mem")).recover {
				case e: Exception => fail("Issue processing datastore test_h2_mem", e)
			}

			assertResult("A\n1\n") { 
				EtlHelper.printDataSource(table.get.read())
			} withClue "Table content doesn't match"
		}
		
		// reload to check if the database was closed and cleaned	
		using(init()) { etl =>
			val store = etl.dataWarehouse.datastores.find(_.name == "test_h2_mem")
				.getOrElse(fail("Issue to find the test_h2_mem datastore"))

			assertThrows[org.h2.jdbc.JdbcSQLSyntaxErrorException]{ 
				EtlHelper.printDataSource(store.table("test_a").get.read())
			} withClue "Shouldn't be able to read the table is in memory"
		}
	}

	
  test("Live test of H2 - filesystem version") {
		using(init()) { etl =>
			val store = etl.dataWarehouse.datastores.find(_.name == "test_h2_fs")
				.getOrElse(fail("Issue to find the test_h2_fs datastore"))

			val table = store.table("test_a")
			assert(table.isDefined) withClue ", table 'test_a' not found"

			Try(etl.runDataStore("test_h2_fs")).recover {
				case e: Exception => fail("Issue processing datastore test_h2_fs", e)
			}

			assertResult("A\n1\n") { 
				EtlHelper.printDataSource(table.get.read())
			} withClue "Table content doesn't match"
		} 
		
		
		// reload to check if the database was closed and cleaned	
		using(init()) { etl =>
			val store = etl.dataWarehouse.datastores.find(_.name == "test_h2_fs")
				.getOrElse(fail("Issue to find the test_h2_fs datastore"))

			val table = store.table("test_a")
				assert(table.isDefined) withClue ", table 'test_a' not found"

			assertResult("A\n1\n") { 
				EtlHelper.printDataSource(table.get.read())
			} withClue "Table content doesn't match"
		}
	}
	
  test("Live test of H2 - server version") {
		// Launch the server
		// -ifNotExists allows the creation of the database by the client
		val server = org.h2.tools.Server.createTcpServer("-ifNotExists").start();
		
		try{
			// First connection
			using(init()) { etl =>
				val store = etl.dataWarehouse.datastores.find(_.name == "test_h2_srv")
					.getOrElse(fail("Issue to find the test_h2_fs datastore"))

				val table = store.table("test_a")
				assert(table.isDefined) withClue ", table 'test_a' not found"

				Try(etl.runDataStore("test_h2_srv")).recover {
					case e: Exception => fail("Issue processing datastore test_h2_srv", e)
				}

				assertResult("A\n1\n") { 
					EtlHelper.printDataSource(table.get.read())
				} withClue "Table content doesn't match"
			} 
			
		}
		finally {
			server.stop()
		}
		
		// Second connection : should not be able to connect as the server is down
		using(init()) { etl =>
			val store = etl.dataWarehouse.datastores.find(_.name == "test_h2_srv")
				.getOrElse(fail("Issue to find the test_h2_srv datastore"))

			val table = store.table("test_a")
				assert(table.isDefined) withClue ", table 'test_a' not found"

				assertThrows[Exception] { 
					EtlHelper.printDataSource(table.get.read())
			} withClue "Should throws as the server is down"
		}
	}
	

}