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
import scala.util.{Try}
import scala.math.BigDecimal

import java.io.StringReader
import java.time.{LocalDate, LocalDateTime}
import java.nio.file.{Files, Paths}
import java.nio.charset.Charset

import javax.mail.internet.MimeMultipart

import play.api.libs.json.Json

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.apache.commons.io.FileUtils

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.Assertions._
import org.scalatest.AppendedClues._

import com.icegreen.greenmail.user.UserException;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;

import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.util.Using._
import com.dataintoresults.etl.core.ProcessResult
import com.dataintoresults.util.TwirlHelper
import org.scalatest.Ignore
import java.io.File



@Ignore
class ProcessMailLiveTest extends FunSuite with BeforeAndAfter {
  private var greenMail : GreenMail = _

  val dw1 = 
    <datawarehouse>
      <datastore name="dw" type="h2"/>
    
      <!-- Defined the source data coming from the web -->
      <!-- Can be read with : ipa read web.earthquake -->
      <datastore name="web" type="http">
        <table name="earthquake" location="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.csv" format="csv" csvHeader="true">
          <column name="time" type="datetime" temporalFormat="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"/>
          <column name="latitude" type="numeric"/>
          <column name="longitude" type="numeric"/>
          <column name="depth" type="numeric"/>
          <column name="mag" type="numeric"/>
          <column name="magType" type="text"/>
          <column name="nst" type="numeric"/>
          <column name="gap" type="numeric"/>
          <column name="dmin" type="numeric"/>
          <column name="rms" type="numeric"/>
          <column name="net" type="text"/>
          <column name="id" type="text"/>
          <column name="update" type="datetime" temporalFormat="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"/>
          <column name="place" type="text"/>
          <column name="type" type="text"/>
          <column name="horizontalError" type="numeric"/>
          <column name="depthError" type="numeric"/>
          <column name="magError" type="numeric"/>
          <column name="magNst" type="numeric"/>
          <column name="status" type="text"/>
          <column name="locationSource" type="text"/>
          <column name="magSource" type="text"/>
        </table>      
      </datastore>
    
      <!-- This module just copy the web.earthquake data source (staging) -->
      <!-- Can be run with : ipa run-module staging_web -->
      <module name="staging_web" datastore="dw"> 
        <table name="earthquake">
          <source type="datastore" datastore="web" table="earthquake"/>
        </table>
      </module>
    
      <!-- Merge the new data with the old one to keep history (archive) -->
      <!-- Can be run with : ipa run-module archive_web -->
      <module name="archive_web" datastore="dw"> 
        <table name="earthquake" strategy="overwrite" businessKeys="time">
          <source type="datastore" datastore="web" table="earthquake"/>
        </table>
      </module>
    
      <!-- Model some (simple) stats tables for reporting -->
      <!-- Can be run with : ipa run-module earthquake -->
      <module name="earthquake" datastore="dw"> 
        <table name="daily_stats">
          <source type="query">
            select cast("time" as date) as "date", count(*) as "earthquake_count", max("mag") as "max_magnitude",
              avg("mag") as "avg_magnitude" 
            from "archive_web"."earthquake"
            group by cast("time" as date)
            order by cast("time" as date) desc
            limit 30
          </source>
        </table>
    
        <table name="weekly_stats">
          <source type="query">
            select to_char(cast("time" as date), 'IYYY-IW') as "date", count(*) as "earthquake_count", max("mag") as "max_magnitude",
              avg("mag") as "avg_magnitude" 
            from "archive_web"."earthquake"
            group by to_char(cast("time" as date), 'IYYY-IW')
            order by to_char(cast("time" as date), 'IYYY-IW') desc
            limit 14
          </source>
        </table>
    
        <table name="last_big_ones">
          <source type="query">
            select "time", "mag", "latitude", "longitude", "place"
            from "archive_web"."earthquake"
            order by "mag" desc
            limit 7
          </source>
        </table>
    
        <table name="mag_vs_depth_corr" type="xlsx" location="earthquake.xlsx"
          sheet="DATA - mag vs depth" colStart="A" rowStart="2">
          <source type="query">
            select "mag", "depth"
            from "archive_web"."earthquake"
            order by "time" desc
            limit 60
          </source>
        </table>
      </module>
      
      <!-- Module that should work -->
      <module name="ok2" datastore="dw">
        <table name="ok" strategy="rebuild">
          <source type="query">
            select 1 as a
          </source>
        </table>
      </module>

      <!-- Module that should FAIL -->
      <module name="nok2" datastore="dw">
        <table name="nok">
          <source type="query">
            select 1/0
          </source>
        </table>
      </module>


      <!-- dw.process.liveEmail.email is set in the config file -->
      <process name="liveEmail">
        <task module="staging_web"/>
        <task module="nok2" onError="warning"/>
        <task module="archive_web"/>
        <task module="earthquake"/>
        <task name="failingTask" module="nok2"/>
        <task module="ok2"/>
      </process>
    </datawarehouse>

    
  test("Live email check (manual)") {
    
		val configPath = "secret/mailer.conf"

		val configFile = new File(configPath)

		if(!configFile.exists)
			cancel(s"you need a configuration file $configPath for this test") 

    val config = ConfigFactory.parseFile(configFile).withFallback(ConfigFactory.load())
    
    using(new EtlImpl(config)) { implicit etl =>
      // Should not thow
      Try(etl.load(dw1)).recover { case e : Exception =>
        fail("Shouldn't throw an exception : " + e.getMessage)
      }

      val result = Try(etl.runProcess("liveEmail")).recover { case e : Exception =>
        fail("Shouldn't throw an exception : " + e.getMessage)
      }.get

      assertResult(ProcessResult.Error)(result.status)
    }
  }

}