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

package com.dataintoresults.etl.datastore.file

import java.io.{File, StringWriter, FileWriter, FileReader, BufferedReader}

import org.scalatest.FunSuite
import org.scalatest.Assertions._

import com.dataintoresults.etl.core.DataSet
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.impl.OverseerBasic


class CSVWriterTest extends FunSuite {
  test("CSVWriterTest - Writing Dataset (StringWriter)") {
    val writer = new StringWriter()
    
    val dataset = DataSet.from(
        Seq("a", "b"), // columns
        Seq(1, 2), 
        Seq("aaa","bbb"))
        
    val dataSink = CSVWriter(columns = dataset.columns).toDataSink(writer)    
    dataset.rows map { r => dataSink.put(r) }
    dataSink.close
    
    assertResult("a\tb\n1\taaa\n2\tbbb\n", "Writen data different from initial data")(writer.toString) 
  }
  
  test("CSVWriterTest - Writing Dataset (real file)") {
    val file = File.createTempFile("com.dataintoresults.etl", "CSVWriterTest") 
    file.deleteOnExit()
    val writer = new FileWriter(file)
    
    val dataset = DataSet.from(
        Seq("a", "b"), // columns
        Seq("# bbb", "a\té à"),
        Seq(1, 2))
        
    val dataSink = CSVWriter(columns = dataset.columns).toDataSink(writer)    
    dataset.rows map { r => dataSink.put(r) }
    dataSink.close
    
    val reader = new BufferedReader(new FileReader(file))
    assertResult("a\tb", "Check line 1")(reader.readLine) 
    assertResult("# bbb\t1", "Check line 2")(reader.readLine) // Shouldn't it be escaped?
    assertResult("\"a\té à\"\t2", "Check line 3")(reader.readLine)
    reader.close
  }

  
  test("CSVWriterTest - Writing Dataset (no writer structure)") {
    val writer = new StringWriter()
    
    val dataset = DataSet.from(
        Seq("a", "b"), // columns
        Seq(1, 2), 
        Seq("aaa","bbb"))
        
    val dataSink = CSVWriter().toDataSink(writer)

    val overseer = new OverseerBasic()
    overseer.runJob(dataset.toDataSource, dataSink)
    
    assertResult("a\tb\n1\taaa\n2\tbbb\n", "Writen data different from initial data")(writer.toString) 
  }
}