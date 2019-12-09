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

package com.dataintoresults.etl.datastore.file

import java.io.Reader


import scala.collection.JavaConversions._

import com.univocity.parsers.csv.{CsvParserSettings, CsvParser}
import com.univocity.parsers.common.ResultIterator

import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.Column


case class CSVReader(
    columns : Seq[Column]  = Array[Column](),
    delimiter : Char = '\t',
    newline : String  = "\n",
    header : Boolean = true,
    quote : Char = '\"',
    quoteEscape : Char = '\\',
    comment : Char = '#'
    ) {
  def toDataSource(reader: Reader) : DataSource = {
    val settings = new CsvParserSettings();
    settings.getFormat().setLineSeparator(newline)
    settings.getFormat().setDelimiter(delimiter)
    settings.setNumberOfRowsToSkip(if(header) 1L else 0L)
    settings.getFormat().setComment(comment)
    settings.getFormat().setQuote(quote)
    settings.getFormat().setQuoteEscape(quoteEscape)
    settings.getFormat().setCharToEscapeQuoteEscaping(quoteEscape)
    
    val parser = new CsvParser(settings);
   
    
		// We create a DataSource on top of the DataSet
		new DataSource {
		  private val iterator =  parser.iterate(reader).iterator
		  
		  def structure = columns
		  
		  def hasNext() = iterator.hasNext()
		  
		  def next(): Seq[Any] = {
		    val row = iterator.next()
		    
		    // Parse each cell with the column info in order to convert from string to 
		    // the appropriate type
		    row zip columns map { case (cell, column) => 
		      column.fromString(cell)
		    }
		    
		  }
		  
		  def close() = reader.close()
		}
  }
}


