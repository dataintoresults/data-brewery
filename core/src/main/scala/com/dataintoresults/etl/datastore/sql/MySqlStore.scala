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

package com.dataintoresults.etl.datastore.sql

import java.sql.DriverManager;
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import scalikejdbc._

import com.typesafe.config.Config

import play.api.Logger

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Etl
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.EtlParameter


import com.dataintoresults.etl.impl.EtlImpl
import com.dataintoresults.etl.impl.ColumnBasic

import com.dataintoresults.etl.core.EtlParameterHelper._


class MySqlStore extends SqlStore {  
  private val logger: Logger = Logger(this.getClass())



   def sqlType = "mysql"
  def jdbcDriver : String = "com.mysql.cj.jdbc.Driver"
  def jdbcUrl : String = createJdbcUrl(host, port, database)
  
  override def defaultConnectionParameters: String = "zeroDateTimeBehavior=convertToNull&autoReconnect=true&characterEncoding=UTF-8&characterSetResults=UTF-8"
  
  def createJdbcUrl(host: String, port: String, database: String) : String = 
    s"jdbc:mysql://${host}:${port}/${database}${connectionParametersUrl}"
  
   override def toString = s"MySqlStore[${name},${host},${user},${password}]"

  override def convertToSqlType(colType: String): String = {
    super.convertToSqlType(colType) match {
      case "varchar" => "varchar(256)" /* MySQL need a limit and over 256 we use text for mysql*/
      case colType: String => colType
    }
  }
  
  
  override protected def jdbcType2EtlType(sqlType: Int, size: Int, decimals: Int) : String= {
    var direct = super.jdbcType2EtlType(sqlType, size, decimals)
    
    /* MySQL limits row size at 65k using the max declared for each varchar.
     * Therefore if it's bigger that 256 we use text. Sound like a good tradeoff */
    if(direct == "text" && size >= 256) 
      direct = "bigtext"
            
    direct
  }
      
  override def defaultPort = "3306"
  
  override def columnEscapeStart = "`" 
  override def columnEscapeEnd = "`" 

  
  override def tableEscapeStart = "`" 
  override def tableEscapeEnd = "`"
  
  override def schemaEscapeStart = "`" 
  override def schemaEscapeEnd = "`" 

   
  /*
   * Need to overload as MySQl take schema as a first parameter
   */
  override def getTableFromDatabaseMetaData(schema: String, table: String) : Table = {
    withDB { db =>
      val databaseMetaData = db.conn.getMetaData();
      
      // Find columns 
      val result = new ResultSetTraversable(databaseMetaData.getColumns(schema, null, table, null));
      
      logger.debug("Request metadata from : " + sqlTablePath(schema, table))
      

      val columns = result map { row =>
        val jdbcType = row.string(6) match {
          case "ENUM" => 12 // VARCHAR
          case _ => row.int(5)
        }
        // See SqlStore for documentation
        new ColumnBasic(row.string(4), jdbcType2EtlType(jdbcType, row.intOpt(7) getOrElse 0, row.intOpt(9) getOrElse 0))  
      }
        
      if(columns.isEmpty) {
        throw new RuntimeException(s"No table ${sqlTablePath(schema, table)} in the datastore ${this.name}. Can't get definition from the datastore.");
      }  
      
      new SqlTable(this, table, schema, columns.toSeq)      
    }
  }
  
   
  /*
   * Need to overload as MySQl take schema as a first parameter
   */
  override def getTablesFromDatabaseMetaData(schema: String) : Seq[String] = {       
    withDB { db =>
      // Access to the metadata of the JDBC connection
      val databaseMetaData = db.conn.getMetaData();
      
      // Find tables 
      val result = new ResultSetTraversable(databaseMetaData.getTables(schema, null, "%", Array("TABLE")));
             
      // Column 3 contains the table names 
      val tables = result map { _.string(3) }
      
      val res = tables.toSeq
      
      logger.debug(s"Discovery from schema $schema (datastore $name) returned tables : ${res.mkString(", ")}")

      res
    }
  }

}

object MySqlStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config) : MySqlStore = {
    val store = new MySqlStore()
    store.parse(dsXml, config)
    store
  }
}