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

import scala.util.{Try, Failure, Success}

import java.io.File
import java.io.IOException
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDate, ZoneOffset}
import java.util.Arrays
import java.io.BufferedReader
import java.io.InputStreamReader

import play.api.Logger

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

import com.google.api.services.analyticsreporting.v4.AnalyticsReportingScopes
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting
import com.google.api.services.analyticsreporting.v4.model.ColumnHeader
import com.google.api.services.analyticsreporting.v4.model.DateRange
import com.google.api.services.analyticsreporting.v4.model.Dimension
import com.google.api.services.analyticsreporting.v4.model.GetReportsRequest
import com.google.api.services.analyticsreporting.v4.model.GetReportsResponse
import com.google.api.services.analyticsreporting.v4.model.Metric
import com.google.api.services.analyticsreporting.v4.model.ReportRequest
import com.google.api.services.analyticsreporting.v4.model.Report
import com.google.api.services.analyticsreporting.v4.model.ReportRow


class GoogleAnalytics { 
  private val logger = Logger(this.getClass)
  private val JSON_FACTORY = GsonFactory.getDefaultInstance()

  private var conn : Option[AnalyticsReporting] = None


  def isConnected() = conn.isDefined

  def connectAsServiceAccount(serviceAccountEmail : String,
                              keyFileLocation : String,
                              applicationName : String) : Unit  =  {
    val httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		val credential = new GoogleCredential.Builder()
			.setTransport(httpTransport)
		  .setJsonFactory(JSON_FACTORY)
			.setServiceAccountId(serviceAccountEmail)
			.setServiceAccountPrivateKeyFromP12File(new File(keyFileLocation))
			.setServiceAccountScopes(AnalyticsReportingScopes.all())
			.build();

		logger.info(s"Connect to Google Analytics with service account $serviceAccountEmail and P12 file $keyFileLocation")

		// Construct the Analytics Reporting service object.
		conn = Some(new AnalyticsReporting.Builder(httpTransport, JSON_FACTORY, credential)
			.setApplicationName(applicationName).build())
  }


  def query(viewId: String,
            metrics: Seq[String],
            dimensions: Seq[String],
            startDate: String = "10daysAgo",
            endDate: String = "3daysAgo",
            rowsPerPage: Int = 10000) = {

		logger.info("Google Analytics report : dim = " + dimensions.mkString(",") + " measures = "  + metrics.mkString(","))
	  
    val dateRange = new DateRange()
		dateRange.setStartDate(startDate)
    dateRange.setEndDate(endDate)
    
    logger.info(s"Google Analytics report : Date range from ${dateRange.getStartDate} to ${dateRange.getEndDate}")

		val metrics2 = metrics.map {m => new Metric().setExpression(m)}
		val dimensions2 = dimensions.map {d => new Dimension().setName(d)}

 

		
    new Iterator[Seq[Object]] {
      private var rowNumber = 0L
      private var lastDate: String = null
      private def service = conn.get

		  private var nextPageToken = Option.empty[String]

      // Create the ReportRequest object.
      val baseRequest = new ReportRequest()
        .setViewId(viewId)
        .setPageSize(rowsPerPage) // Max value for GA. 10k row per pages.
        .setDateRanges(Arrays.asList(dateRange))
        .setDimensions(dimensions2.asJava)
        .setMetrics(metrics2.asJava);
		
      // Call the first batch
      private var rowIterator : Iterator[Seq[Object]] = nextBatch()

      def hasNext() = {
        if(rowIterator.hasNext)
          true
        // If we are at the end of a batch we load a new one
        else if(nextPageToken.isDefined) {
          logger.info(s"Google Ananalytics : Loading next request")
          rowIterator = nextBatch()
          rowIterator.hasNext
        }
        else
          false
      }

      def next(): Seq[Object] = rowIterator.next()


      // Returning a Try[T] wrapper
      @annotation.tailrec
      def retry[T](n: Int)(fn: => T): Try[T] = {
        Try { fn } match {
          case Failure(e) if n > 1 => {
            logger.warn(s"Error during the call ${e}. Let's retry (${n-1} attempts left).")
            // Wait a second
            Thread.sleep(1000L)
            retry(n - 1)(fn)
          }
          case s => s
        }
      }

      private def nextBatch() = {
        val request = nextPageToken.map { token => baseRequest.setPageToken(token) } getOrElse baseRequest

        // Create the GetReportsRequest object.
        val getReport = new GetReportsRequest()
          .setReportRequests(List(request).asJava)

        val report = retry(5){
          service.reports().batchGet(getReport).execute().getReports().get(0)
        } match {
          case Failure(e) => throw e
          case Success(s) => s
        }

        nextPageToken = if(report.getNextPageToken() != null) {
          logger.info(s"Google Ananalytics : There will be a next request to be made, token ${report.getNextPageToken()}")
          Some(report.getNextPageToken())
        } else {
          logger.info(s"Google Ananalytics : no next request to be made")
          None
        }

        parseReport(report)
      }

      
      private def parseReport(report: Report): Iterator[Seq[Object]] = {    
        val datasetBuilder = Vector.newBuilder[Seq[Object]]
        val header : ColumnHeader= report.getColumnHeader()
        val dimensionHeaders = header.getDimensions().asScala		  
        val metricHeaders = header.getMetricHeader().getMetricHeaderEntries().asScala		  
        val rows: Seq[ReportRow] = report.getData().getRows().asScala		  

        if (rows == null) {
          logger.info("No data found for Google analytics viewId " + viewId);
          Seq.empty[Seq[Object]].iterator
        }
        else {
          val dateLog = if(lastDate != null) s", last date: $lastDate" else ""
          logger.info(s"Fetching ${rows.length} Google analytics rows (already read: $rowNumber $dateLog)")
          val rowBuilder = Vector.newBuilder[Object]
          for(row <- rows) {
            val dimensionsRows = row.getDimensions();
            val metricsRows = row.getMetrics();
            var i, j , k = 0;

            for(i <- 0 to dimensionHeaders.size -1 if i < dimensionsRows.size) {
              if(dimensions(i).equals("ga:date"))
                lastDate = dimensionsRows.get(i)
              rowBuilder += dimensionsRows.get(i)
            }

            for(j <- 0 to metricsRows.size()-1) {
              val values = metricsRows.get(j);
              for (k <- 0 to values.getValues().size()-1 if k < metricHeaders.size) {
                
                val str: String = values.getValues().get(k)			
                metricHeaders(k).getType match {
                  case "INTEGER" => 
                    rowBuilder += Long.box(str.toLong) // should box to ensure it's an object
                  case "FLOAT" => 
                    rowBuilder += new java.math.BigDecimal(str)
                  case "CURRENCY" => 
                    rowBuilder += new java.math.BigDecimal(str)
                  case _ =>  throw new RuntimeException(s"Can only process Google Analytics measures of type INTEGER, here it's ${metricHeaders(k).getType}")
                }
                  
              }
            }

            rowNumber += 1
            datasetBuilder += rowBuilder.result
            rowBuilder.clear
          }
          datasetBuilder.result.iterator
        }        
      }
    }
  }
}
