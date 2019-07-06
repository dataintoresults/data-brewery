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

package com.dataintoresults.etl.datastore.googleSheet

import scala.collection.JavaConversions._

import java.io.File
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat

import play.api.libs.json._
import play.api.Logger

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.Spreadsheet;
import com.google.api.services.sheets.v4.model.Sheet;
import com.google.api.services.sheets.v4.model.UpdateCellsRequest;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.api.services.sheets.v4.model.CellData;
import com.google.api.services.sheets.v4.model.CellFormat;
import com.google.api.services.sheets.v4.model.ExtendedValue;
import com.google.api.services.sheets.v4.model.NumberFormat;
import com.google.api.services.sheets.v4.model.BatchUpdateSpreadsheetRequest
import com.google.api.services.sheets.v4.model.GridCoordinate
import com.google.api.services.sheets.v4.model.RowData
import com.google.api.services.sheets.v4.model.Request

import org.apache.poi.ss.usermodel.DateUtil

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.impl.DataSetImpl

import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

import com.dataintoresults.util.XmlHelper._

class GoogleSheetStore extends EtlDatastore with DataStore {
  private val logger: Logger = Logger(this.getClass())
	
	private val _serviceAccountEmail = EtlParameter[String](nodeAttribute="serviceAccountEmail", configAttribute="dw.datastore."+name+".serviceAccountEmail")
	private val _keyFileLocation = EtlParameter[String](nodeAttribute="keyFileLocation", configAttribute="dw.datastore."+name+".keyFileLocation")
	private val _applicationName = EtlParameter[String](nodeAttribute="applicationName", configAttribute="dw.datastore."+name+".applicationName")

	def serviceAccountEmail = _serviceAccountEmail.value
	def keyFileLocation = _keyFileLocation.value
	def applicationName = _applicationName.value

	private val _tables = EtlChilds[GoogleSheetTable]()

	override def toString = s"GoogleSheetStore[${_name}]" 
	
  def close(): Unit = {}

  def tables: Seq[Table] = _tables
  
  def spreadsheet(name: String) : Option[GoogleSheetTable] = _tables.find(_.name == name)
	
	def createDataSource(gsTable: GoogleSheetTable) : DataSource = {
	  val sheets = getConnection()
	  
	  val range = (if(gsTable.sheet.size > 0) "'" + gsTable.sheet + "'!" else "" ) + gsTable.colStart + ":" + lastColumn(gsTable)
	  
	  logger.info("Google Sheet range : " + range)
	  	  
		val datasetBuilder = Vector.newBuilder[Seq[Any]]
	  
	  var r = 0
	  var stop = false
	  val rowStart = gsTable.rowStart.toInt
	  
	  val response = sheets.spreadsheets().values()
	  // With UNFORMATTED_VALUE we get numbers and date as java.math.BigDecimal
			.get(gsTable.spreadsheetId, range).setValueRenderOption("UNFORMATTED_VALUE")
			.execute();
		val values = response.getValues();
		if (values == null || values.size() == 0) {			
	    logger.info("Google Sheet : No data found")	  	  
		} else {
			values.toSeq foreach  { row =>
			  r += 1
  		  if(r >= rowStart && !stop) {
  		    val formattedRow : Seq[Any] = (gsTable.columns zipAll (row, null, null)) map { case (col, c) => c match {
  		      case "" => null /* Age old problem is "" null or a string? Let's pretend is always for null */
  		      case s : String =>
  		        col.basicType match {
  		          case Column.TEXT => s
  		          case Column.BIGTEXT => s
  		          case _ => throw new RuntimeException(s"For Google Sheet table ${name}.${gsTable.name} for row ${r} and column ${col.name}, it's a text field ('$s')and shouldn't be for this type.")
  		        }
  		      case d : java.math.BigDecimal => 
  		        col.basicType match {
  		          case Column.BIGINT => d.longValue()
  		          case Column.INT => d.intValue()
  		          case Column.NUMERIC => d
  		          case Column.DATETIME => DateUtil.getJavaDate(d.doubleValue());
  		          case Column.DATE => DateUtil.getJavaDate(d.doubleValue().floor); // What is after the decimal point is hours and minutes
  		          case Column.BIGTEXT => d.toString();
  		          case Column.TEXT => d.toString();
  		          case _ => throw new RuntimeException(s"For Google Sheet table ${name}.${gsTable.name} for row ${r} and column ${col.name}, it's a numeric field and shouldn't be for this type.")
  		        }
  		      case _ => null /* Empty case */
  		    }
  		    }  
  		    
  				datasetBuilder += formattedRow
  		  }
			}
		}
		
		val dataset = new DataSetImpl(gsTable.columns, datasetBuilder.result())

		// We create a DataSource on top of the DataSet
		new DataSource {
		  private val iterator = dataset.rows.iterator
		  
		  def structure = dataset.columns
		  
		  def hasNext() = iterator.hasNext()
		  
		  def next() = iterator.next()
		  
		  def close() = {}
		}
	  
	}
	
	
	
	def createDataSink(gsTable: GoogleSheetTable) : DataSink = {
	  val sheets = getConnection()
	  
	  val range = (if(gsTable.sheet.size > 0) "'" + gsTable.sheet + "'!" else "" ) + gsTable.colStart + gsTable.rowStart
	  
	  val colIndex = gsTable.colStart.toUpperCase().zip(Stream.from(gsTable.colStart.size, -1)).foldRight(0 /* idx default = 1*/) { case ((c, i), sum) =>
	    sum + (i match {
	      case 1 => (c - 'A')	      
	      case 2 => (c - 'A' + 1)*26
	      case _ => throw new RuntimeException(s"The Google Sheet column ${gsTable.colStart} for table ${name}.${gsTable.name} is to far to be used.")
	    })
	  }
	  
	  // Google sheet index start at and not at 1 like spreadsheet
	  val rowIndex = gsTable.rowStart.toInt -1
	  
	  // Find the sheetId
	  val lowerCaseSheetName = gsTable.sheet.toLowerCase
	  val sheetId : Int = 
	    if(gsTable.sheet.size == 0) 0
	    else {
	      val spreadsheet = sheets.spreadsheets().get(gsTable.spreadsheetId).execute()
	      val sheet : Option[Sheet] = spreadsheet.getSheets() find (_.getProperties.getTitle.toLowerCase == lowerCaseSheetName)
	      sheet match {
	        case Some(s) => s.getProperties.getSheetId
	        case None => throw new RuntimeException(s"The Google Sheet sheet  ${gsTable.sheet} is not found in ${name}.${gsTable.name}.")
	      }
	    }
	  	  
		val datasetBuilder = Vector.newBuilder[Seq[Any]]
	  	  
		// We create a DataSink to store data then update when it's closed
		new DataSink {		  
	    private var rows : java.util.ArrayList[RowData] = new java.util.ArrayList[RowData](1000)
		  def structure = gsTable.columns
		  		  
		  def put(row: Seq[Any]) : Unit = {
	      if(rows.size()> 10000)
	        throw new RuntimeException("You can only submit up to 10000 rows to a Google Sheet")
	      	      
	      rows.add(new RowData().setValues(row map { cell =>
	        cell match {
	        case null => new CellData()	
                .setUserEnteredValue(new ExtendedValue()
                        .setStringValue(""))
	        case t : java.sql.Timestamp => {
	          val cell = new CellData();
            cell.setUserEnteredValue(new ExtendedValue().setNumberValue(DateUtil.getExcelDate(t)));
            cell.setUserEnteredFormat(
              new CellFormat().setNumberFormat(new NumberFormat().setType("DATE")));   
            cell
	        }
	        case s : String =>  new CellData()	
                .setUserEnteredValue(new ExtendedValue()
                        .setStringValue(s))
	        case l : Long =>  new CellData()	
                .setUserEnteredValue(new ExtendedValue()
                        .setNumberValue(l))	        
	        case i : Int =>  new CellData()	
                .setUserEnteredValue(new ExtendedValue()
                        .setNumberValue(i))	        
          case d : java.math.BigDecimal => 
            new CellData()	
                .setUserEnteredValue(new ExtendedValue()
                        .setNumberValue(d.doubleValue))
                .setUserEnteredFormat(new CellFormat().setNumberFormat(new NumberFormat().setType("NUMBER")));    
          case d : Double =>  new CellData()	
                .setUserEnteredValue(new ExtendedValue()
                        .setNumberValue(d))
	        case o : Object => {
	          new CellData()	
                .setUserEnteredValue(new ExtendedValue()
                        .setStringValue(o.toString))
                       
	        }
	      }}))
	    } 
		  
		  def close() = {    
	      logger.debug("Google Sheet: close DataSink")
	  	  		  
        val requests = new java.util.ArrayList[Request]();

        requests.add(new Request()
                .setUpdateCells(new UpdateCellsRequest()
                        .setStart(new GridCoordinate()
                                .setSheetId(sheetId)
                                .setRowIndex(rowIndex)
                                .setColumnIndex(colIndex))
                        .setRows(rows)
                        .setFields("*")));

        val batchUpdateRequest = new BatchUpdateSpreadsheetRequest()
                .setRequests(requests);
        
    	  val response = sheets.spreadsheets().batchUpdate(gsTable.spreadsheetId, batchUpdateRequest)
                .execute();
		  }
		}
	  
	}
	
	
	private def lastColumn(gsTable: GoogleSheetTable) : String = {
	  val firstColumn = gsTable.colStart
	  
	  (firstColumn.head + gsTable.columns.length -1).asInstanceOf[Char].toString
	}
	
	private def getConnection() : Sheets = {
		connectAsServiceAccount(serviceAccountEmail, keyFileLocation, applicationName)
	}

	private def connectAsServiceAccount(serviceAccountEmail : String
			, keyFileLocation : String, applicationName : String) : Sheets  =  {   
					val httpTransport = GoogleNetHttpTransport.newTrustedTransport();
					val jsonFactory = JacksonFactory.getDefaultInstance();

					val credential = new GoogleCredential.Builder()
							.setTransport(httpTransport)
							.setJsonFactory(jsonFactory)
							.setServiceAccountId(serviceAccountEmail)
							.setServiceAccountPrivateKeyFromP12File(new File(keyFileLocation))
							.setServiceAccountScopes(SheetsScopes.all())
							.build();

					// Construct the Analytics Reporting service object.
					new Sheets.Builder(httpTransport, jsonFactory, credential)
					.setApplicationName(applicationName)
					.build();
	}


	override def test() : Boolean = {
			true
	}
	
}


object GoogleSheetStore {
	def fromXml(dsXml : scala.xml.Node, config: com.typesafe.config.Config) : GoogleSheetStore = {
		val store = new GoogleSheetStore()
		store.parse(dsXml, config)
		store
	}
	
	def main(args : Array[String]) : Unit = {
	  val xml = <datastore name="sheets" type="googleSheet">
					<table name="test_primaire" spreadsheetId="115Cg13ZfKvOHFVQUoDRD_eaCW0nSViv5AUiuTDXMZ6w" colStart="A" rowStart="2" sheet="Feuille 1">
					</table>	
			</datastore>
	  
	  val ds = fromXml(xml.head,  ConfigFactory.load())
	  
	  val spreadsheet = ds.spreadsheet("test_primaire").get
	  
	  val sink = ds.createDataSink(spreadsheet)
	  
	  sink.put(Seq("aab","df"))
	  sink.put(Seq(null,"dfg"))
	  sink.put(Seq(9.3,"sdfsdf"))
	  sink.close()
	}
}
