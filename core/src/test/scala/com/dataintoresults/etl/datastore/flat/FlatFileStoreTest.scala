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

package com.dataintoresults.etl.datastore.flat

import java.io.{File, BufferedReader, FileReader, StringReader}
import java.nio.file.{Paths}

import org.scalatest.FunSuite
import org.scalatest.Assertions._

import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.impl.EtlImpl
import com.dataintoresults.etl.core.Etl
import com.dataintoresults.util.Using._

class FlatFileStoreTest extends FunSuite {
  val tempFile = File.createTempFile(getClass.getCanonicalName, "")
  tempFile.deleteOnExit()
  val tempFile2 = File.createTempFile(getClass.getCanonicalName, "")
  tempFile2.deleteOnExit()
 
  val dwh =     
    <datawarehouse>
      <datastore name="files" type="flat">
        <table name="simple_csv" type="csv" location={new File(getClass.getResource("csv.txt").getPath()).toPath().toString()}>
					<column name="c1" type="string"/>
					<column name="c2" type="int"/>
				</table>
        <table name="compressed_csv" type="csv" location={new File(getClass.getResource("csv.txt.gz").getPath).toPath().toString()} compression="gz">
					<column name="c1" type="string"/>
					<column name="c2" type="string"/>
				</table>
      </datastore>
      <datastore name="files_relative_path" type="flat" location={new File(getClass.getResource("csv.txt").getPath).toPath().toString().dropRight(7)}>
        <table name="simple_csv" type="csv" location="csv.txt">
					<column name="c1" type="string"/>
					<column name="c2" type="string"/>
				</table>
      </datastore>
      <datastore name="multiple_files" type="flat" location={new File(getClass.getResource("csv.txt").getPath).toPath().toString().dropRight(7)}>
        <table name="multiple_csv" type="csv" location="csv*.txt">
					<column name="c1" type="string"/>
					<column name="c2" type="string"/>
				</table>
      </datastore>
      <datastore name="unconventional" type="flat">
        <table name="unconventional" type="csv" comment="_" delimiter=";" quote="$" quoteEscape="£" header="false" location={new File(getClass.getResource("delimiter.csv").getPath()).toPath().toString()}>
					<column name="c1" type="string"/>
					<column name="c2" type="int"/>
				</table>
      </datastore>
      <datastore name="files_write" type="flat">
        <table name="simple_csv" type="csv" location={tempFile.getPath}>
					<column name="d1" type="string"/>
					<column name="d2" type="int"/>
					<source type="datastore" datastore="files" table="simple_csv"/>
				</table>
        <table name="no_structure_csv" type="csv" location={tempFile2.getPath}>
					<source type="datastore" datastore="files" table="simple_csv"/>
				</table>
      </datastore>
    </datawarehouse>
  
  
  test("Parsing a good configuration") {    
		using(new EtlImpl()) { implicit etl =>
      var start = System.currentTimeMillis
      // Should not thow
      try {
        etl.load(dwh)
      }
      catch {
        case e: Exception => {
          e.printStackTrace
          fail("Shouldn't throw an exception : " + e.getMessage)
        }
      }
      assertResult(1, "There should be a datastore named files") {
        etl.dataWarehouse.datastores.find(_.name == "files").productArity
      }
      assertResult(2, "There should be one table in datastore files") {
        etl.findDataStore("files").tables.length
      }
		} 
  }
  
  test("Reading a simple CSV") {    
		using(new EtlImpl()) { implicit etl =>
      var start = System.currentTimeMillis
      // Should not thow
      try {
        etl.load(dwh)
      }
      catch {
        case e: Exception => fail("Shouldn't throw an exception at spec parsing : " + e.getMessage)
      }
      assertResult("c1, c2\ntoto, 1\ntata, 2") {
        EtlHelper.printDataset(etl.previewTableFromDataStore("files", "simple_csv", 10))
      }
		}
  }
  
  test("Reading a relative path CSV") {    
		using(new EtlImpl()) { implicit etl =>
      var start = System.currentTimeMillis
      // Should not thow
      try {
        etl.load(dwh)
      }
      catch {
        case e: Exception => fail("Shouldn't throw an exception at spec parsing : " + e.getMessage)
      }
      assertResult("c1, c2\ntoto, 1\ntata, 2") {
        EtlHelper.printDataset(etl.previewTableFromDataStore("files_relative_path", "simple_csv", 10))
      }
		}
  }
  
  
  test("Reading a relative path with multiple CSV (wildcard)") {    
		using(new EtlImpl()) { implicit etl =>
      var start = System.currentTimeMillis
      // Should not thow
      try {
        etl.load(dwh)
      }
      catch {
        case e: Exception => fail("Shouldn't throw an exception at spec parsing : " + e.getMessage)
      }
      assertResult("c1, c2\ntoto, 1\ntata, 2\ntoto, 3\ntata, 4") {
        EtlHelper.printDataset(etl.previewTableFromDataStore("multiple_files", "multiple_csv", 10))
      }
		}
  }
  
  
  test("Reading an unconventional CSV") {    
		using(new EtlImpl()) { implicit etl =>
      var start = System.currentTimeMillis
      // Should not thow
      try {
        etl.load(dwh)
      }
      catch {
        case e: Exception => fail("Shouldn't throw an exception at spec parsing : " + e.getMessage)
      }
      assertResult("c1, c2\ntoto, 1\nta$ta, 2") {
        EtlHelper.printDataset(etl.previewTableFromDataStore("unconventional", "unconventional", 10))
      }
		}
  }
  
  
  test("Reading a gzipped CSV") {    
		using(new EtlImpl()) { implicit etl =>
      var start = System.currentTimeMillis
      // Should not thow
      try {
        etl.load(dwh)
      }
      catch {
        case e: Exception => fail("Shouldn't throw an exception at spec parsing : " + e.getMessage)
      }
      assertResult("c1, c2\ntoto, 1\ntata, 2") {
        EtlHelper.printDataset(etl.previewTableFromDataStore("files", "compressed_csv", 10))
      }
		}
  }
    
  test("Writing a simple CSV") {    
		using(new EtlImpl()) { implicit etl =>
      try {
        etl.load(dwh)
      }
      catch {
        case e: Exception => cancel("Error parsing ETL")
      }
      // Should not thow
      try {
        etl.runDataStoreTable("files_write", "simple_csv")
      }
      catch {
        case e: Exception => fail("Shouldn't fail at writing csv : " + e.getMessage)
      }
            
      val reader = new BufferedReader(new FileReader(tempFile))
      assertResult("d1\td2", "Check line 1")(reader.readLine) 
      assertResult("toto\t1", "Check line 2")(reader.readLine)
      assertResult("tata\t2", "Check line 3")(reader.readLine)
      reader.close
		}
  }

  test("Writing a simple CSV (no structure given)") {    
		using(new EtlImpl()) { implicit etl =>
      try {
        etl.load(dwh)
      }
      catch {
        case e: Exception => cancel("Error parsing ETL")
      }
      // Should not thow
      try {
        etl.runDataStoreTable("files_write", "no_structure_csv")
      }
      catch {
        case e: Exception => fail("Shouldn't fail at writing csv : " + e.getMessage)
      }
            
      val reader = new BufferedReader(new FileReader(tempFile2))
      assertResult("c1\tc2", "Check line 1")(reader.readLine) 
      assertResult("toto\t1", "Check line 2")(reader.readLine)
      assertResult("tata\t2", "Check line 3")(reader.readLine)
      reader.close
		}
  }
}