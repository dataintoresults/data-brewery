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

package com.dataintoresults.etl.core

import scala.reflect.runtime.universe._
import com.dataintoresults.etl.impl.DataSetColumnar
import com.dataintoresults.etl.impl.ColumnBasic

/**
 * Represent a set of data in memory with direct access to all data.
 * 
 * If the set of data is big, it's better to use a DataSource.
 *
 */
trait DataSet {
  /**
   * Returns the structure of the dataset.
   */
  def columns : Seq[Column]
  
  /**
   * Returns the rows underlying this dataset.
   */
  def rows : Seq[Seq[Any]]

  /**
   * Returns the number of rows of this dataset.
   */
  def size = rows.size

  /**
   * Convert the dataset to a data source.
   */
  def toDataSource: DataSource = new DataSource {      
    private val iterator = rows.iterator
    
    def structure : Seq[Column] = columns
    
    def hasNext() : Boolean = iterator.hasNext

    def next() : Seq[Any] = iterator.next
    
    def close() : Unit = {}
  }
}

object DataSet {
  def from[C1: TypeTag](cols: Seq[String], col1: Seq[C1]): DataSet = {
    if(cols.length != 1)
      throw new RuntimeException("Using DataSet.from[1] without giving 1 column.")
    
    val type_1 = ColumnBasic.columnTypeFromScalaType[C1]
    
    new DataSetColumnar(Seq(new ColumnBasic(cols.head, type_1)), Seq(col1))
  }
  
  def from[C1: TypeTag, C2: TypeTag](cols: Seq[String], col1: Seq[C1], col2: Seq[C2]): DataSet = {
    if(cols.length != 2)
      throw new RuntimeException("Using DataSet.from[2] without giving 2 columns.")
    
    if(col1.length != col2.length)
      throw new RuntimeException(s"Using DataSet.from[2] with columns of different length [${col1.length}, ${col2.length}].")    
    
    val colsData = Seq(col1, col2)
        
    val colsType = Seq(ColumnBasic.columnTypeFromScalaType[C1], 
      ColumnBasic.columnTypeFromScalaType[C2])
    
    val columns = cols zip colsType map { case (name, tpe) => new ColumnBasic(name, tpe) }
    
    new DataSetColumnar(columns, colsData)
  }
  
  
  def from[C1: TypeTag, C2: TypeTag, C3: TypeTag](cols: Seq[String], col1: Seq[C1], col2: Seq[C2], col3: Seq[C3]): DataSet = {
    if(cols.length != 3)
      throw new RuntimeException("Using DataSet.from[3] without giving 3 columns.")
    
    if(col1.length != col2.length || col1.length != col3.length)
      throw new RuntimeException(s"Using DataSet.from[3] with columns of different length [${col1.length}, ${col2.length}, ${col3.length}].")    
    
    val colsData = Seq(col1, col2, col3)
        
    val colsType = Seq(ColumnBasic.columnTypeFromScalaType[C1], 
      ColumnBasic.columnTypeFromScalaType[C2], 
      ColumnBasic.columnTypeFromScalaType[C3])
    
    val columns = cols zip colsType map { case (name, tpe) => new ColumnBasic(name, tpe) }
    
    new DataSetColumnar(columns, colsData)
  }
  
  def from[C1: TypeTag, C2: TypeTag, C3: TypeTag, C4: TypeTag](cols: Seq[String], 
      col1: Seq[C1], col2: Seq[C2], col3: Seq[C3], col4: Seq[C4]): DataSet = {
    if(cols.length != 4)
      throw new RuntimeException("Using DataSet.from[4] without giving 4 columns but ${cols.length}.")
    
    if(col1.length != col2.length || col1.length != col3.length || col1.length != col4.length)
      throw new RuntimeException(s"Using DataSet.from[4] with columns of different length [${col1.length}, ${col2.length}, ${col3.length}, ${col4.length}].")    
    
    val colsData = Seq(col1, col2, col3, col4)
        
    val colsType = Seq(ColumnBasic.columnTypeFromScalaType[C1], 
      ColumnBasic.columnTypeFromScalaType[C2], 
      ColumnBasic.columnTypeFromScalaType[C3], 
      ColumnBasic.columnTypeFromScalaType[C4])
    
    val columns = cols zip colsType map { case (name, tpe) => new ColumnBasic(name, tpe) }
    
    new DataSetColumnar(columns, colsData)
  }
  
  def from[C1: TypeTag, C2: TypeTag, C3: TypeTag, C4: TypeTag, C5: TypeTag](cols: Seq[String], 
      col1: Seq[C1], col2: Seq[C2], col3: Seq[C3], col4: Seq[C4], col5: Seq[C5]): DataSet = {
    if(cols.length != 5)
      throw new RuntimeException("Using DataSet.from[4] without giving 5 columns but ${cols.length}.")
    
    if(col1.length != col2.length || col1.length != col3.length || col1.length != col4.length || col1.length != col5.length)
      throw new RuntimeException(s"Using DataSet.from[5] with columns of different length [${col1.length}, ${col2.length}, ${col3.length}, ${col4.length}, ${col5.length}].")    
    
    val colsData = Seq(col1, col2, col3, col4, col5)
        
    val colsType = Seq(ColumnBasic.columnTypeFromScalaType[C1], 
      ColumnBasic.columnTypeFromScalaType[C2], 
      ColumnBasic.columnTypeFromScalaType[C3], 
      ColumnBasic.columnTypeFromScalaType[C4], 
      ColumnBasic.columnTypeFromScalaType[C5])
    
    val columns = cols zip colsType map { case (name, tpe) => new ColumnBasic(name, tpe) }
    
    new DataSetColumnar(columns, colsData)
  }
}