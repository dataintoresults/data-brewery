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

import com.typesafe.config.Config

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Etl
import com.dataintoresults.etl.core.Table

import com.dataintoresults.etl.impl.EtlImpl

import com.dataintoresults.etl.core.EtlParameterHelper._


class MySqlStore extends SqlStore {	
 	def sqlType = "mysql"
	def jdbcDriver : String = "com.mysql.jdbc.Driver"
	def jdbcUrl : String = createJdbcUrl(host, port, database)
	
	
	def createJdbcUrl(host: String, port: String, database: String) : String = 
	  s"jdbc:mysql://${host}:${port}/${database}?useSSL=false&zeroDateTimeBehavior=convertToNull&autoReconnect=true&characterEncoding=UTF-8&characterSetResults=UTF-8&useServerPrepStmts=false&rewriteBatchedStatements=true"
	
 	override def toString = s"MySqlStore[${name},${host},${user},${password}]"

  override def convertToSqlType(colType: String): String = {
    super.convertToSqlType(colType) match {
      case "varchar" => "varchar(256)" /* MySQL need a limit and over 256 we use text for mysql*/
      case colType: String => colType
    }
  }
  
  
	override protected def jdbcType2EtlType(sqlType: Int, size: Int) : String= {
	  var direct = super.jdbcType2EtlType(sqlType, size)
	  
	  /* MySQL limits row size at 65k using the max declared for each varchar.
	   * Therefore if it's bigger that 256 we use text. Sound like a good tradeoff */
	  if(direct == "varchar" && size > 256) 
	    direct = "text"
	    	    
	  direct
	}
      
	override def defaultPort = "3306"
	
	override def columnEscapeStart = "`" 
	override def columnEscapeEnd = "`" 
}

object MySqlStore {
	def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config) : DataStore = {
		val store = new MySqlStore()
		store.parse(dsXml, config)
		store
	}
}