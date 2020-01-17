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

package com.dataintoresults.etl.datastore.googleAnalytics

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
class GoogleAnalyticsTest extends FunSuite {
  test("Test of google analytics - View Id parameter on datastore") {
    // ViewId on datastore
    val xml = 
      <datastore name="test_analytics" type="googleAnalytics"
        serviceAccountEmail="test@test.com"
        keyFileLocation="somewhere"
        viewId="123">
        <table name="sessions" startDate="2019-01-01" endDate="2019-01-02">
          <column name="date" type="varchar" gaName="ga:date" gaType="dimension"/>  
          <column name="sessions" type="bigint" gaName="ga:sessions" gaType="measure"/>
        </table>
      </datastore>

    val store = GoogleAnalyticsStore.fromXml(xml, ConfigFactory.load())
    val table = store.table("sessions")
    assert(table.isDefined) withClue ", table 'session' not found"

    val tableAnalytics = table.get.asInstanceOf[GoogleAnalyticsTable]

    assertResult("123") {
      tableAnalytics.viewId
    } 
  }

  
  test("Test of google analytics - View Id parameter on table") {
    // ViewId on datastore
    val xml = 
      <datastore name="test_analytics" type="googleAnalytics"
        serviceAccountEmail="test@test.com"
        keyFileLocation="somewhere"
        viewId="123">
        <table name="sessions" startDate="2019-01-01" endDate="2019-01-02" viewId="456">
          <column name="date" type="varchar" gaName="ga:date" gaType="dimension"/>  
          <column name="sessions" type="bigint" gaName="ga:sessions" gaType="measure"/>
        </table>
      </datastore>

    val store = GoogleAnalyticsStore.fromXml(xml, ConfigFactory.load())
    val table = store.table("sessions")
    assert(table.isDefined) withClue ", table 'session' not found"

    val tableAnalytics = table.get.asInstanceOf[GoogleAnalyticsTable]

    assertResult("456") {
      tableAnalytics.viewId
    }
  }

  test("Test of google analytics - View Id parameter is missing") {
    // ViewId on datastore
    val xml = 
      <datastore name="test_analytics" type="googleAnalytics"
        serviceAccountEmail="test@test.com"
        keyFileLocation="somewhere">
        <table name="sessions" startDate="2019-01-01" endDate="2019-01-02">
          <column name="date" type="varchar" gaName="ga:date" gaType="dimension"/>  
          <column name="sessions" type="bigint" gaName="ga:sessions" gaType="measure"/>
        </table>
      </datastore>

    val store = GoogleAnalyticsStore.fromXml(xml, ConfigFactory.load())
    val table = store.table("sessions")
    assert(table.isDefined) withClue ", table 'session' not found"

    val tableAnalytics = table.get.asInstanceOf[GoogleAnalyticsTable]

    assertThrows[RuntimeException](tableAnalytics.viewId) withClue ", should have thrown an exception because viewId is missing"
  }

}