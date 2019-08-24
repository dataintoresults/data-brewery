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


class EtlImplTest extends FunSuite {
	val dw = 
		<datawarehouse>
			<include path="store/dw.xml"/>
			<include path="module/business.xml"/>
		</datawarehouse>

	val store_dw = <datastore name="dw" type="postgresql" host="127.0.0.1" user="joe" password="test"/>

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
	val merged_dw = 
		<datawarehouse>
			<datastore name="dw" type="postgresql" host="127.0.0.1" user="joe" password="test">
			</datastore>
			<module name="business" datastore="dw">
				<table name="d_date">
					<source type="query"><![CDATA[select 1 as a, 2 as b, 1<2 as c]]></source>
				</table>
			</module>
		</datawarehouse>
		
  test("Etl need to preprocess include elements and contentPath attributes") {
		// Create files in a temp repertory
		val basePath = Files.createTempDirectory("com.dataintoresults.etl.impl.EtlImplTest")
		try {
			val storePath = Files.createDirectory(basePath.resolve("store"))
			val modulePath = Files.createDirectory(basePath.resolve("module"))
			val businessPath = Files.createDirectory(modulePath.resolve("business"))

			val dwPath = basePath.resolve("dw.xml")

			XML.save(dwPath.toString(), dw)
			XML.save(storePath.resolve("dw.xml").toString(), store_dw)
			XML.save(modulePath.resolve("business.xml").toString(), module_business)

			Files.write(businessPath.resolve("d_date_query.sql"), sql_query.getBytes)
			
			val etl = new EtlImpl()
			etl.load(dwPath)

			val printer = new scala.xml.PrettyPrinter(80, 2)
			assertResult(printer.format(etl.save()))(printer.format(merged_dw))
		}
		finally {
			FileUtils.deleteDirectory(basePath.toFile())
		}
	}
}