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

import java.io.StringReader

import org.scalatest.FunSuite

import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.util.EtlHelper


class CSVReaderTest extends FunSuite {
  test("Parsing a TSV file (should be with default config)") {
    val reader = new StringReader("a\tb\naaa\t3\nbbb\t6")
    val structure = Array(new ColumnBasic("c1", "string"), new ColumnBasic("c2","int"))
    val parser = CSVReader(columns = structure)

    assertResult("c1, c2\naaa, 3\nbbb, 6\n") {
      EtlHelper.printDataSource(parser.toDataSource(reader))
    }
  }

  test("Parsing a TSV file with no header") {
    val reader = new StringReader("aaa\tbbb\n3\t6")
    val structure = Array(new ColumnBasic("c1", "string"), new ColumnBasic("c2","string"))
    val parser = CSVReader(columns = structure, header = false)
    
    assertResult("c1, c2\naaa, bbb\n3, 6\n") {
      EtlHelper.printDataSource(parser.toDataSource(reader))
    }
  }
  
  test("Parsing a CSV file") {
    val reader = new StringReader("a,b\naaa,bbb\n3,6")
    val structure = Array(new ColumnBasic("c1", "string"), new ColumnBasic("c2","string"))
    val parser = CSVReader(columns = structure, delimiter = ',')
    
    assertResult("c1, c2\naaa, bbb\n3, 6\n") {
      EtlHelper.printDataSource(parser.toDataSource(reader))
    }
  }
  
  test("Handling delimiter in quoted cell") {
    val reader = new StringReader(
"""a,"b,n "
aaa,"b,bb"
3,6
""")
    val structure = Array(new ColumnBasic("c1", "string"), new ColumnBasic("c2","string"))
    val parser = CSVReader(columns = structure, delimiter = ',')
    
    assertResult("c1, c2\naaa, b,bb\n3, 6\n") {
      EtlHelper.printDataSource(parser.toDataSource(reader))
    } 
  }
  
  test("Handling quote in quoted cell") {
    val reader = new StringReader("a,b\naaa,\"b\\\"bb\"\n3,6\n")
    val structure = Array(new ColumnBasic("c1", "string"), new ColumnBasic("c2","string"))
    val parser = CSVReader(columns = structure, delimiter = ',')
    
    assertResult("c1, c2\naaa, b\"bb\n3, 6\n") {
      EtlHelper.printDataSource(parser.toDataSource(reader))
    }
  }

  test("Parsing a timestamp") {
    val reader = new StringReader("a\n2019-11-22 12:23:05")
    val structure = Array(new ColumnBasic("c1", "datetime"))
    val parser = CSVReader(columns = structure)

    val ds = parser.toDataSource(reader)
    val df = EtlHelper.dataSourceToDataset(ds)

    assertResult("c1\n2019-11-22T12:23:05") {
      EtlHelper.printDataset(df)
    }

    assertResult(12) {
      val ts = df.rows(0)(0).asInstanceOf[java.time.LocalDateTime]
      ts.getHour()
    }
  }
}