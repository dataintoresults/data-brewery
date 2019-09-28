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

package com.dataintoresults.etl.util

import play.api.libs.json._

import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSet
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.impl.DataSetImpl

object EtlHelper {
  def printDataset(dataset: DataSet) : String = {
    val header = dataset.columns map { _.name } mkString ", "
    val body = dataset.rows map { _ mkString ", " }  mkString "\n"
    
    header + "\n" + body
  }
  
  def printDataSource(dataSource: DataSource) : String = {
    val header = dataSource.structure map { _.name } mkString ", "
    
    val sb = new StringBuffer(header)
    sb append "\n"
        
    dataSource.foreach { row => 
      sb.append (row.map({ c: Any => 
        c match {
          case null => ""
          case _ => c.toString()}
        }).mkString(", ")
        + "\n") 
    }

    dataSource.close() 
    
    sb.toString()    
  }
  
  def dataSourceToDataset(ds: DataSource) : DataSet = {
    val structure = ds.structure
    val content = ds.map(x => x).toSeq
    new DataSetImpl(structure, content)
  }

  def datasetToXml(dataset: DataSet) : scala.xml.Node = {
    val result = <dataset>
				<header>
          { dataset.columns map { c => <column name={c.name} type={c.colType}/>  } }
				</header>
				<rows>
          { dataset.rows map { r => <row> { r map { c => <cell>{if(c!= null) c.toString else ""}</cell> }  } </row>  } }
				</rows>
			</dataset>;
    return result;
  }
  
  
  def datasetToJson(dataset: DataSet) : JsObject = {
    val result = Json.obj(
      "columns" -> { dataset.columns map { c => c.name } } ,      
      "rows" -> { dataset.rows map { r => { r map ( { c => 
        c match {
          case null => JsString("")
          case o: JsObject => o
          case x: Object => JsString(x.toString)
        }})
      }}});
    return result;
  }
  
  def datasetToJsonStream(dataset: DataSet) : String = {
    dataset.rows map { r => 
      { JsObject(((r zip dataset.columns) map {
        case (null, c) => (c.name -> JsNull)
        case (o: JsObject, c) => (c.name -> o)
        case (x: Object, c) => (c.name -> JsString(x.toString))
        case (_, _) => throw new RuntimeException("Issue to convert the dataset to Json, missing a column")
        }))
      }
    } map { r => Json.stringify(r) } mkString "\n"
  }
}