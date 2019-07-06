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
 *	   expectedContent : "What to expect from the table session"
 *   }
 * }
 * 
 */
@Slow
class GoogleAnalyticsLiveTest extends FunSuite {
  test("Live test of google analytics - simple query") {

		val xml = 
			<datastore name="test_analytics" type="googleAnalytics" applicationName="Unit Test">
				<table name="sessions" startDate="2019-01-01" endDate="2019-01-02">
					<column name="date" type="varchar" gaName="ga:date" gaType="dimension"/>  
					<column name="sessions" type="bigint" gaName="ga:sessions" gaType="measure"/>
				</table>
			</datastore>

		val configFile = new File("secret/googleAnalytics/googleAnalytics.conf")

		if(!configFile.exists)
			cancel("you need a configuration file secret/googleAnalytics/googleAnalytics.conf for this test") 

		val config = ConfigFactory.parseFile(configFile)  


		Seq("viewId", "keyFileLocation", "serviceAccountEmail", "sessions.expectedContent").foreach { confKey =>
			if(!config.hasPath(s"dw.datastore.test_analytics.$confKey"))
				cancel(s"Test need config value dw.datastore.test_analytics.$confKey to be set")
		}

		val expectedContent = config.getString("dw.datastore.test_analytics.sessions.expectedContent")

		val store = GoogleAnalyticsStore.fromXml(xml, config)
		val table = store.table("sessions")
		assert(table.isDefined) withClue ", table 'session' not found"

		assertResult(expectedContent) {
			EtlHelper.printDataSource(table.get.read())
		} withClue ", returned data wasn't what was expected"
  }

}