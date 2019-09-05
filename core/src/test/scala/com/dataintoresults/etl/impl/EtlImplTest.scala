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
import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.datastore.sql.SqlStore


class EtlImplTest extends FunSuite {
	val dw1 = 
		<datawarehouse>
			<include path="store/dw.xml"/>
			<include path="module/business.xml"/>
		</datawarehouse>

	val dw2 = 
		<datawarehouse>
			<include path="store/*.xml"/>
			<include path="store_*.xml"/>
		</datawarehouse>

	val store_dw = <datastore name="dw" type="postgresql" host="127.0.0.1" user="joe" password="test"/>
	val store_hubspot = <datastore name="hubspot" type="hubspot" apiKey="aaaaaa-bbb-ccc-ddddddddddddd"/>

	val module_business = 
		<module name="business" datastore="dw">
			<table name="d_date">
				<source type="query" contentPath="business/d_date_query.sql"><t/></source>
			</table>
		</module>

		
	val sql_query = """
		select 1 as a,
			2 as b,
			1<2 as c	
		"""

	val merged_dw1 = 
		<datawarehouse>
			<datastore name="dw" type="postgresql" host="127.0.0.1" user="joe" password="test"/>
			<module name="business" datastore="dw">
				<table name="d_date">
					<source type="query"><![CDATA[select 1 as a, 2 as b, 1<2 as c]]></source>
				</table>
			</module>
		</datawarehouse>

	val merged_dw2 = 
		<datawarehouse>
			<datastore name="dw" type="postgresql" host="127.0.0.1" user="joe" password="test"/>
			<datastore name="hubspot" type="hubspot" apiKey="aaaaaa-bbb-ccc-ddddddddddddd"/>
			<datastore name="dw" type="postgresql" host="127.0.0.1" user="joe" password="test"/>
			<datastore name="hubspot" type="hubspot" apiKey="aaaaaa-bbb-ccc-ddddddddddddd"/>
		</datawarehouse>
		
  test("Etl need to preprocess include elements and contentPath attributes") {
		// Create files in a temp repertory
		val basePath = Files.createTempDirectory("com.dataintoresults.etl.impl.EtlImplTest")
		try {
			val storePath = Files.createDirectory(basePath.resolve("store"))
			val modulePath = Files.createDirectory(basePath.resolve("module"))
			val businessPath = Files.createDirectory(modulePath.resolve("business"))

			val dwPath = basePath.resolve("dw.xml")

			XML.save(dwPath.toString(), dw1)
			XML.save(storePath.resolve("dw.xml").toString(), store_dw)
			XML.save(modulePath.resolve("business.xml").toString(), module_business)

			Files.write(businessPath.resolve("d_date_query.sql"), sql_query.getBytes)
			
			val etl = new EtlImpl()
			etl.load(dwPath)

			val printer = new scala.xml.PrettyPrinter(80, 2)
			assertResult(printer.format(merged_dw1))(printer.format(etl.save()))
		}
		finally {
			FileUtils.deleteDirectory(basePath.toFile())
		}
	}

  test("Etl need to preprocess include elements with wildcard path") {
		// Create files in a temp repertory
		val basePath = Files.createTempDirectory("com.dataintoresults.etl.impl.EtlImplTest2")
		try {
			val storePath = Files.createDirectory(basePath.resolve("store"))

			val dwPath = basePath.resolve("dw.xml")

			XML.save(dwPath.toString(), dw2)

			XML.save(storePath.resolve("dw.xml").toString(), store_dw)
			XML.save(storePath.resolve("hubspot.xml").toString(), store_hubspot)

			
			XML.save(basePath.resolve("store_dw.xml").toString(), store_dw)
			XML.save(basePath.resolve("store_hubspot.xml").toString(), store_hubspot)
			
			val etl = new EtlImpl()
			etl.load(dwPath)

			val printer = new scala.xml.PrettyPrinter(80, 2)

			assertResult(printer.format(merged_dw2))(printer.format(etl.save()))
		}
		finally {
			FileUtils.deleteDirectory(basePath.toFile())
		}
	}



	val xmlProcess = 
		<datawarehouse>
			<datastore name="dw" type="h2">
				<table name="test">
					<source type="query"><![CDATA[select 2 as a, 2 as b, 1<2 as c]]></source>
				</table>
			</datastore>
			<module name="business" datastore="dw">
				<table name="d_date">
					<source type="query"><![CDATA[select 1 as a, 2 as b, 1<2 as c]]></source>
				</table>
			</module>
			<process name="dummy">
				<task datastore="dw"/>
				<task module="business"/>
			</process>
		</datawarehouse>
	
  test("Etl ability to process 'process' elements") {
		val etl = new EtlImpl()
		etl.load(xmlProcess)

		etl.findDataStore("dw").asInstanceOf[SqlStore].dropSchemaIfExists("business")

		etl.runProcess("dummy") 

		//
		assertResult("A, B, C\n1, 2, 1", "Check module processing") {
			EtlHelper.printDataset(etl.runQuery("dw", "select * from business.d_date"))
		}

		assertResult("A, B, C\n2, 2, 1", "Check datastore processing") {
			EtlHelper.printDataset(etl.runQuery("dw", "select * from test"))
		}


	}
}