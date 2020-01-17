/*******************************************************************************
 *
 * Copyright (C) 2020 by Obsidian SAS : https://dataintoresults.com/
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

package com.dataintoresults.etl.datastore.googleSearch

import java.io.File


import scala.xml._

import com.typesafe.config.ConfigFactory

import org.scalatest.FunSuite 
import org.scalatest.tags.Slow
import org.scalatest.AppendedClues._

import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.util.EtlHelper

/**
 * Live test of Google Analytics.
 * 
 * In order not to be canceled, you need to provide configuration to a
 * testing Google Analytics view
 * 
 * dw.datastore.test_analytics {  
 *   viewId : "1234567.."
 *   keyFileLocation : "Location to a .p12 credential"
 *   serviceAccountEmail : "The Google service account to be used"
 *   sessions {
 *     expectedContent : "What to expect from the table session"
 *   }
 * }
 * 
 */  
@Slow
class GoogleSearchLiveTest extends FunSuite {
  test("Live test of google search - simple query") {

    val xml =  
      <datastore name="test_seo" type="googleSearch"> 
        <table name="seo" startDate="2019-01-01" endDate="2019-01-02">
          <column name="query" type="text"/>  
          <column name="impressions" type="bigint"/>
        </table>
      </datastore>

		val configFile = new File("secret/googleSearch/googleSearch.conf")
		

    if(!configFile.exists)
      cancel("you need a configuration file secret/googleSearch/googleSearch.conf for this test") 

    val config = ConfigFactory.parseFile(configFile)  

    Seq("seo.property", "keyFileLocation", "serviceAccountEmail", "seo.expectedLinesCount").foreach { confKey =>
      if(!config.hasPath(s"dw.datastore.test_seo.$confKey"))
        cancel(s"Test need config value dw.datastore.test_seo.$confKey to be set")
    }

    val expectedLinesCount = config.getInt("dw.datastore.test_seo.seo.expectedLinesCount")

    val store = GoogleSearchStore.fromXml(xml, config)
    val table = store.table("seo")
    assert(table.isDefined) withClue ", table 'seo' not found"

    assertResult(expectedLinesCount) {
      table.get.read().size
    } withClue ", returned data wasn't what was expected"
  }

}