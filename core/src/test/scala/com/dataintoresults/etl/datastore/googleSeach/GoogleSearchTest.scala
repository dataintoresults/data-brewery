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
 * Test of Google Analytics.
 * 
 */
@Slow
class GoogleSearchTest extends FunSuite {
  test("Test of google search -  parameter on datastore") {
    // ViewId on datastore
    val xml = 
      <datastore name="test_analytics" type="googleSearch"
        serviceAccountEmail="test@test.com"
        keyFileLocation="somewhere"
        property="test.com">
        <table name="seo" startDate="2019-01-01" endDate="2019-01-02">
        </table>
      </datastore>

    val store = GoogleSearchStore.fromXml(xml, ConfigFactory.load())
    val table = store.table("seo")
    assert(table.isDefined) withClue ", table 'seo' not found"

    val tableSEO = table.get.asInstanceOf[GoogleSearchTable]

    assertResult("test.com") {
      tableSEO.property
    } 
  }

  
  test("Test of google analytics - View Id parameter on table") {
    // ViewId on datastore
    val xml = 
      <datastore name="test_analytics" type="googleSearch"
        serviceAccountEmail="test@test.com"
        keyFileLocation="somewhere"
        property="test.com">
        <table name="seo" startDate="2019-01-01" endDate="2019-01-02" property="toto.com">
        </table>
      </datastore>

    val store = GoogleSearchStore.fromXml(xml, ConfigFactory.load())
    val table = store.table("seo")
    assert(table.isDefined) withClue ", table 'seo' not found"

    val tableSEO = table.get.asInstanceOf[GoogleSearchTable]

    assertResult("toto.com") {
      tableSEO.property
    }
  }

  test("Test of google analytics - View Id parameter is missing") {
    // ViewId on datastore
    val xml = 
      <datastore name="test_analytics" type="googleSearch"
        serviceAccountEmail="test@test.com"
        keyFileLocation="somewhere">
        <table name="seo" startDate="2019-01-01" endDate="2019-01-02">
        </table>
      </datastore>

    val store = GoogleSearchStore.fromXml(xml, ConfigFactory.load())
    val table = store.table("seo")
    assert(table.isDefined) withClue ", table 'seo' not found"

    val tableSEO = table.get.asInstanceOf[GoogleSearchTable]

    assertThrows[RuntimeException](tableSEO.property) withClue ", should have thrown an exception because property is missing"
  }

}