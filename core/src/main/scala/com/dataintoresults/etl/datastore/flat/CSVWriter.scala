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

package com.dataintoresults.etl.datastore.flat

import java.io.Writer

import scala.collection.JavaConversions._

import com.univocity.parsers.csv.{CsvWriterSettings, CsvWriter}
import com.univocity.parsers.common.ResultIterator

import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Column


case class CSVWriter(
    columns : Seq[Column]  = Array[Column](),
    delimiter : Char = '\t',
    newline : String  = "\n",
    header : Boolean = true,
    quote : Char = '\"',
    quoteEscape : Char = '\\',
    comment : Char = '#'
    ) {
  def toDataSink(writer: Writer) : DataSink = {
    val settings = new CsvWriterSettings();
    settings.getFormat().setLineSeparator(newline)
    settings.getFormat().setDelimiter(delimiter)
    settings.getFormat().setQuote(quote)
    settings.getFormat().setComment(comment)
    settings.getFormat().setQuoteEscape(quoteEscape)
    settings.getFormat().setCharToEscapeQuoteEscaping(quoteEscape)
    
    val csvWriter = new CsvWriter(writer, settings);
    
    if(header) 
      csvWriter.writeHeaders(columns map { c => c.name })
       
    
		// We create a DataSource on top of the DataSet
		// We create a DataSink to store data then update when it's closed
		new DataSink {		  
		  def structure = columns
		  		  
		  def put(row: Seq[Any]) : Unit = {
	      csvWriter.writeRow(structure zip row map { case (c, v) => c.toString(v) }:_*)
	    } 
		  
		  def close() = {
		    csvWriter.close
		  }
		}
  }
}


