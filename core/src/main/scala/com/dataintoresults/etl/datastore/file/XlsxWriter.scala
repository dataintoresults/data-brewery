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

import java.nio.file.Path

import scala.Option

import play.api.Logger

import org.apache.poi.xssf.usermodel.{XSSFWorkbook, XSSFWorkbookFactory}

import scala.collection.JavaConversions._

import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Column
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import com.dataintoresults.etl.core.Column.TEXT
import com.dataintoresults.etl.core.Column.DATE
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import com.dataintoresults.etl.core.Column.BIGINT
import java.time.LocalDate
import java.time.LocalDateTime
import java.nio.file.Files
import org.apache.poi.ss.format.CellFormat
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator


case class XlsxWriter(
    columns: Seq[Column],
    sheetName: String,
    colStart: String,
    rowStart: Int
    ) {      
  private val logger: Logger = Logger(this.getClass())

  def toDataSink(path: Path) : DataSink = {
    val workbook = Files.exists(path) match {
      case true => new XSSFWorkbook(Files.newInputStream(path))// open in read-only
      case false => new XSSFWorkbook()
    }
    
    // get an existing sheet or create one
    val sheet = Option(workbook.getSheet(sheetName)).getOrElse(workbook.createSheet(sheetName))
    val colStartIndex = ExcelHelper.colToIndex(colStart)

    val dateStyle = workbook.createCellStyle();
    // see https://poi.apache.org/apidocs/dev/org/apache/poi/ss/usermodel/BuiltinFormats.html
    dateStyle.setDataFormat(0xe);

    val dateTimeStyle = workbook.createCellStyle();
    // see https://poi.apache.org/apidocs/dev/org/apache/poi/ss/usermodel/BuiltinFormats.html
    dateTimeStyle.setDataFormat(0x16)

    // We create a DataSource
    new DataSink {
      // Minux 2 because the API is 0-based, the Excel sheet is 1-based
      // and the first hasNext call will increment by 1
      private var currentRow = rowStart-2
      
      private var _structure = columns

      def structure = _structure

      /*
       * If we don't have a struture, we delay until something comes in
       */
	    override def setIncomingStruture(incomingStructure: Seq[Column]): Unit = {
        if(structure.isEmpty) 
          _structure = incomingStructure
        else {
          super.setIncomingStruture(incomingStructure)
        }

        // Clean target
        val lastRowNum = sheet.getLastRowNum()
        // +1 as hasNext will increment automatically by 1
        (currentRow+1) until lastRowNum foreach { i =>
          Option(sheet.getRow(i)).foreach { row =>
            row
              // Keep those on left of the table
              .filter(_.getColumnIndex() >= colStartIndex)
              // Keep those on right of the table
              .filter(_.getColumnIndex() < colStartIndex + structure.size)
              // Remove those in the table space
              .foreach(row.removeCell(_))
          }
        }
      }
		  		  
		  def put(rowData: Seq[Any]) : Unit = { 
        currentRow += 1
        val row = Option(sheet.getRow(currentRow)).getOrElse(sheet.createRow(currentRow))
        
        structure.zip(rowData).zipWithIndex.map { case ((col, content), i) => 
          val cell = Option(row.getCell(i+colStartIndex)).getOrElse(row.createCell(i+colStartIndex))
          cell.setCellValue(1)
          if(content == null)
            cell.setBlank()
          else { 
            col.basicType match {
              case Column.BIGINT => cell.setCellValue(content.asInstanceOf[Long])
              case Column.BIGTEXT => cell.setCellValue(content.asInstanceOf[String])
              case Column.BOOLEAN => cell.setCellValue(content.asInstanceOf[Boolean])
              case Column.DOUBLE => cell.setCellValue(content.asInstanceOf[Double])
              case Column.DATE => {
                cell.setCellStyle(dateStyle)
                cell.setCellValue(content.asInstanceOf[LocalDate])
              }
              case Column.DATETIME =>  {
                cell.setCellStyle(dateTimeStyle)
                cell.setCellValue(content.asInstanceOf[LocalDateTime])
              }
              case Column.INT => cell.setCellValue(content.asInstanceOf[Int])
              case Column.LAZY => cell.setCellValue(content.toString())
              case Column.NUMERIC => cell.setCellValue(content.asInstanceOf[java.math.BigDecimal].doubleValue)
              case Column.TEXT => cell.setCellValue(content.asInstanceOf[String])
              case Column.VARIANT => cell.setCellValue(content.toString())
            }
          }
        }
      } 
      		  
		  def close() = {
        // Update formulas before closing the file
        XSSFFormulaEvaluator.evaluateAllFormulaCells(workbook)
        val outputStream = Files.newOutputStream(path)
        workbook.write(outputStream)
        outputStream.close
        workbook.close
      }

    }
  }
}


