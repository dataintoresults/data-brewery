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

package com.dataintoresults.etl.datastore.googleSearchConsole

import java.io.File
import java.io.IOException
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDate, ZoneOffset}
import java.util.logging.Logger
import java.util.Arrays
import java.io.BufferedReader
import java.io.InputStreamReader


import scala.collection.JavaConverters._
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.HttpTransport
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.webmasters.Webmasters
import com.google.api.services.webmasters.model.{SearchAnalyticsQueryRequest, ApiDataRow }


class GoogleSearch {
  private val logger = Logger.getLogger(this.getClass.getName)
  private val JSON_FACTORY = GsonFactory.getDefaultInstance()
  private final val REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"
  private final val OAUTH_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"
  private final val CLICKS_IDX = -1
  private final val IMPRESSIONS_IDX = -2
  private final val POSITION_IDX = -3
  private final val PROPERTY_IDX = -10
  private val notDimensions = Seq("clicks", "impressions", "position", "property")

  private var conn : Option[Webmasters] = None

  private val rowsPerBatch = 25000

  private final val defaultValidationCallback = (url : String) => {
    System.out.println("Please open the following URL in your browser then type the authorization code:")
    System.out.println("  " + url)
    System.out.println("Enter authorization code:")
    val br = new BufferedReader(new InputStreamReader(System.in))
    br.readLine()
  }


  def isConnected() = conn.isDefined

  def connectAsServiceAccount(serviceAccountEmail : String,
                              keyFileLocation : String,
                              applicationName : String) : Unit  =  {
    val httpTransport = GoogleNetHttpTransport.newTrustedTransport()
    val credential = new GoogleCredential.Builder()
      .setTransport(httpTransport)
      .setJsonFactory(JSON_FACTORY)
      .setServiceAccountId(serviceAccountEmail)
      .setServiceAccountPrivateKeyFromP12File(new File(keyFileLocation))
      .setServiceAccountScopes(Arrays.asList(OAUTH_SCOPE))
      .build()

    // Construct the Analytics Reporting service object.
    conn = Some(new Webmasters.Builder(httpTransport, JSON_FACTORY, credential)
      .setApplicationName(applicationName)
      .build())
  }

  def connectAsUser(clientId : String,
                    clientSecret : String,
                    validationCallback : (String => String) = defaultValidationCallback,
                    applicationName : String = "dummyApp") : Unit  =  {
    val httpTransport = GoogleNetHttpTransport.newTrustedTransport()
    val flow = new GoogleAuthorizationCodeFlow.Builder(
      httpTransport, JSON_FACTORY, clientId, clientSecret, Arrays.asList(OAUTH_SCOPE))
      .setAccessType("online")
      .setApprovalPrompt("auto").build()

    val url = flow.newAuthorizationUrl().setRedirectUri(REDIRECT_URI).build()

    val code = validationCallback(url)
    val response = flow.newTokenRequest(code).setRedirectUri(REDIRECT_URI).execute()
    val credential = new GoogleCredential().setFromTokenResponse(response)

    // Construct the Analytics Reporting service object.
    conn = Some(new Webmasters.Builder(httpTransport, JSON_FACTORY, credential)
      .setApplicationName(applicationName)
      .build())
  }


  def query(property: String,
            columns: Seq[String] = Seq("query", "date", "clicks", "impressions", "position"),
            startDate: String = "10daysAgo",
            endDate: String = "3daysAgo",
            maxRows: Int = 1000) = {

    val dimensions = columns.filterNot(notDimensions.contains(_))
    val columnMapping = columns map {
      _ match {
        case "clicks" => CLICKS_IDX
        case "impressions" => IMPRESSIONS_IDX
        case "position" => POSITION_IDX
        case "property" => PROPERTY_IDX
        case dim: String => dimensions.indexOf(dim)
      }
    }

    new Iterator[Seq[Object]] {
      private var rowNumber = 0
      private var rowIterator : Iterator[ApiDataRow] = nextBatch()
      private def service = conn.get

      def hasNext() = {
        if(rowIterator.hasNext)
          true
        // If we are at the end of a batch we load a new one
        else if(rowNumber % rowsPerBatch == 0) {
          rowIterator = nextBatch()
          rowIterator.hasNext
        }
        else
          false
      }

      def next(): Seq[Object] = {
        val row = rowIterator.next()
        rowNumber += 1
        columnMapping map { _ match {
            case CLICKS_IDX => Int.box(row.getClicks.toInt)
            case IMPRESSIONS_IDX => Int.box(row.getImpressions.toInt)
            case POSITION_IDX => row.getPosition
            case PROPERTY_IDX => property
            case i: Int => row.getKeys.get(i)
          }
        }
      }

      private def nextBatch() = {
        val query = new SearchAnalyticsQueryRequest()
        query.setStartDate(parseDate(startDate))
        query.setEndDate(parseDate(endDate))
        query.setRowLimit(rowsPerBatch)
        query.setStartRow(rowNumber)
        query.setDimensions(dimensions.asJava)
        query.setAggregationType("byPage")

        val result = service.searchanalytics().query(property, query).execute()
        result.getRows.iterator().asScala
      }
    }
  }

  private lazy val dateRegex = """(\d\d\d\d)-(\d\d)-(\d\d)""".r
  private lazy val agoRegex = """(\d+)day.?Ago""".r
  private def parseDate(date: String): String = {
    date match {
      case dateRegex(_) => date
      case agoRegex(ago) =>
        LocalDate.now(ZoneOffset.UTC)
          .minusDays(ago.toInt)
          .format(DateTimeFormatter.ISO_LOCAL_DATE)
    }
  }
}

