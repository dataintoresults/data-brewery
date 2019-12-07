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

import java.nio.file.Path

import org.apache.poi.xssf.usermodel.{XSSFWorkbook, XSSFWorkbookFactory}

import scala.collection.JavaConversions._

import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.Column
import org.apache.poi.ss.usermodel.CellType
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy
import com.dataintoresults.etl.core.Column.TEXT
import com.dataintoresults.etl.core.Column.DATE


case class XlsxReader(
    columns: Seq[Column],
    sheetName: String,
    colStart: String,
    rowStart: Int
    ) {
  def toDataSource(path: Path) : DataSource = {
    val workbook = XSSFWorkbookFactory.createWorkbook(path.toFile, true) // open in read-only
    val sheet = workbook.getSheet(sheetName);
    val colStartIndex = ExcelHelper.colToIndex(colStart)
    val lastSheetRowNum = sheet.getLastRowNum()

    // We create a DataSource
    new DataSource {
      // Minux 2 because the API is 0-based, the Excel sheet is 1-based
      // and the first hasNext call will increment by 1
      private var currentRow = rowStart-2
      
      def structure = columns
      
      def hasNext() = {
        currentRow += 1

        // Check with sheet metadata for a first approximation
        // If the row is empty that's not good as well
        if(currentRow > lastSheetRowNum || sheet.getRow(currentRow) == null)
          false
        else 
          true
      }
      
      def next(): Seq[Any] = {
        val row = sheet.getRow(currentRow)
        // Parse each cell with the column info in order to convert from string to 
        // the appropriate type
        columns.zipWithIndex.map { case (column, i) => 
          val cell = row.getCell(i + colStartIndex, MissingCellPolicy.CREATE_NULL_AS_BLANK)
          cell.getCellType() match {
            case CellType.BLANK => null
            case CellType.ERROR => null
            case CellType.STRING =>
              column.basicType match {
                case Column.TEXT => cell.getStringCellValue()
                case Column.BIGTEXT => cell.getStringCellValue()
                case _ => throw new RuntimeException(s"Can't convert a text value in Excel to type ${column.colType}")
              }
            case CellType.NUMERIC =>
              column.basicType match {
                case Column.NUMERIC => cell.getNumericCellValue()
                case Column.INT => cell.getNumericCellValue().toInt
                case Column.BIGINT => cell.getNumericCellValue().toLong
                case Column.DATETIME => cell.getLocalDateTimeCellValue()
                case Column.DATE => cell.getLocalDateTimeCellValue().toLocalDate()
                case _ => throw new RuntimeException(s"Can't convert a text value in Excel to type ${column.colType}")
              }
            case CellType.FORMULA => throw new RuntimeException(s"Excel reader can't currently handle formula cells")
            case CellType.BOOLEAN => throw new RuntimeException(s"Excel reader can't currently handle boolean cells")
            case CellType._NONE => throw new RuntimeException(s"Excel reader can't currently handle _NONE cells")
          }
        }
        
      }
      
      def close() = workbook.close()
    }
  }
}


