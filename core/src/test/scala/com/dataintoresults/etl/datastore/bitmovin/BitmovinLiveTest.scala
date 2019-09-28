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

package com.dataintoresults.etl.datastore.bitmovin

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
 * testing Google Analytics view (the GUID below are mockup and doesn't work)
 * 
 * dw.datastore.test_bitmovin {	 
 *	apiKey="35111605-1ecc-46d0-bbc0-04f6550a5fb2" 
 *	licenseKey="28c12cfd-34af-4abf-b216-75b2f94191c5"
 *	sessions {
 *		expectedContent : "date, country, nb_impressions\n2019-09-24, LU, 1\n2019-09-24, FR, 29\n2019-09-24, DE, 1\n2019-09-22, FR, 8\n"
 *	}
 *	granular {
 *		expectedCount : 332
 *	}
 *	startuptime {
 *		expectedContent : "startuptime\n498.5\n"
 *	}
 *	played {
 *		expectedContent : "played\n8302.847682119205\n"
 *	}
}
 * 
 */
@Slow
class BitmovinLiveTest extends FunSuite {
  test("Live test of bitmovin - big query") {

		val xml = 
			<datastore name="test_bitmovin" type="bitmovin"> 
				<table name="granular" startDate="2019-09-20" endDate="2019-09-25">
					<column name="date" type="datetime" formula="minute"/>
					<column name="video_start" type="bigint" formula="VIDEOTIME_START"/>
					<column name="video_end" type="bigint" formula="VIDEOTIME_END"/>
					<column name="video_duration" type="bigint" formula="VIDEO_DURATION"/>					
					<column name="duration" type="bigint" formula="DURATION"/> 
					<column name="impression_id" type="varchar" formula="impression_id"/>
					<column name="user_id" type="varchar" formula="user_id"/>
					<column name="video_id" type="varchar" formula="video_id"/>
					<column name="path" type="varchar" formula="path"/>
					<column name="duration" type="bigint" formula="count(DURATION)"/>
				</table>
			</datastore>

		val configFile = new File("secret/bitmovin/bitmovin.conf")

		if(!configFile.exists)
			cancel("you need a configuration file secret/bitmovin/bitmovin.conf for this test") 

		val config = ConfigFactory.parseFile(configFile)   

		Seq("apiKey", "licenseKey", "granular.expectedCount").foreach { confKey =>
			if(!config.hasPath(s"dw.datastore.test_bitmovin.$confKey"))
				cancel(s"Test need config value dw.datastore.test_bitmovin.$confKey to be set")
		}

		val expectedCount = config.getInt("dw.datastore.test_bitmovin.granular.expectedCount")

		val store = BitmovinStore.fromXml(xml, config)
		val table = store.table("granular")
		assert(table.isDefined) withClue ", table 'granular' not found"
		
		assertResult(expectedCount) {
			EtlHelper.dataSourceToDataset(table.get.read()).size
		} withClue ", returned data wasn't what was expected"
  }

  test("Live test of bitmovin - count query") {
		val xml = 
			<datastore name="test_bitmovin" type="bitmovin">
				<table name="sessions" startDate="2019-09-20" endDate="2019-09-25">
					<column name="date" type="date" formula="day"/>
					<column name="country" type="varchar" formula="country"/>
					<column name="nb_impressions" type="bigint" formula="count(impression_id)"/>
				</table>
			</datastore>

		val configFile = new File("secret/bitmovin/bitmovin.conf")

		if(!configFile.exists)
			cancel("you need a configuration file secret/bitmovin/bitmovin.conf for this test") 

		val config = ConfigFactory.parseFile(configFile)  

		Seq("apiKey", "licenseKey", "sessions.expectedContent").foreach { confKey =>
			if(!config.hasPath(s"dw.datastore.test_bitmovin.$confKey"))
				cancel(s"Test need config value dw.datastore.test_bitmovin.$confKey to be set")
		}

		val expectedContent = config.getString("dw.datastore.test_bitmovin.sessions.expectedContent")

		val store = BitmovinStore.fromXml(xml, config)
		val table = store.table("sessions")
		assert(table.isDefined) withClue ", table 'session' not found"

		assertResult(expectedContent) {
			EtlHelper.printDataSource(table.get.read())
		} withClue ", returned data wasn't what was expected"
	}
	
  test("Live test of bitmovin - avg played query (with filter and no interval)") {
		val xml = 
			<datastore name="test_bitmovin" type="bitmovin">
				<table name="played" startDate="2019-09-20" endDate="2019-09-25">
					<filter formula="played &gt; 0"/>
					<column name="played" type="double" formula="avg(played)"/>
				</table>
			</datastore>

		val configFile = new File("secret/bitmovin/bitmovin.conf")

		if(!configFile.exists)
			cancel("you need a configuration file secret/bitmovin/bitmovin.conf for this test") 
 
		val config = ConfigFactory.parseFile(configFile)  

		Seq("apiKey", "licenseKey", "played.expectedContent").foreach { confKey =>
			if(!config.hasPath(s"dw.datastore.test_bitmovin.$confKey"))
				cancel(s"Test need config value dw.datastore.test_bitmovin.$confKey to be set")
		}

		val expectedContent = config.getString("dw.datastore.test_bitmovin.played.expectedContent")

		val store = BitmovinStore.fromXml(xml, config)
		val table = store.table("played")
		assert(table.isDefined) withClue ", table 'played' not found"

		assertResult(expectedContent) { 
			EtlHelper.printDataSource(table.get.read())
		} withClue(", returned data wasn't what was expected")
	}
	
	test("Live test of bitmovin - percentile query") {
		val xml = 
			<datastore name="test_bitmovin" type="bitmovin">
				<table name="startuptime" startDate="2019-09-20" endDate="2019-09-25">
					<filter formula="startuptime &gt; 0"/>
					<column name="startuptime" type="double" formula="percentile(startuptime, 50)"/>
				</table>
			</datastore>

		val configFile = new File("secret/bitmovin/bitmovin.conf")

		if(!configFile.exists)
			cancel("you need a configuration file secret/bitmovin/bitmovin.conf for this test") 

		val config = ConfigFactory.parseFile(configFile)  

		Seq("apiKey", "licenseKey", "startuptime.expectedContent").foreach { confKey =>
			if(!config.hasPath(s"dw.datastore.test_bitmovin.$confKey"))
				cancel(s"Test need config value dw.datastore.test_bitmovin.$confKey to be set")
		}

		val expectedContent = config.getString("dw.datastore.test_bitmovin.startuptime.expectedContent")

		val store = BitmovinStore.fromXml(xml, config)
		val table = store.table("startuptime")
		assert(table.isDefined) withClue ", table 'startuptime' not found"

		assertResult(expectedContent) {
			EtlHelper.printDataSource(table.get.read())
		} withClue ", returned data wasn't what was expected"
  }

}