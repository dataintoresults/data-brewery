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

import play.api.Logger

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Etl
import com.dataintoresults.etl.core.Table

import com.dataintoresults.etl.impl.EtlImpl

import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.core.EtlParameter
import scala.util.Try
import scalikejdbc._

class OracleStore extends SqlStore {
  private val logger: Logger = Logger(this.getClass())
  
  def sqlType = "oracle"
  def jdbcDriver : String = "oracle.jdbc.OracleDriver"
  def jdbcUrl : String = createJdbcUrl(host, port, database)
  
  private val _defaultTablespace = EtlParameter[String](nodeAttribute="defaultTablespace", configAttribute="dw.datastore."+name+".defaultTablespace", defaultValue="USERS")

  def defaultTablespace = _defaultTablespace.value
  
  def defaultPort = "1521"
  
  def createJdbcUrl(host: String, port: String, database: String) : String = 
    s"jdbc:oracle:thin:@//${host}:${port}/${database}"
  
  override def toString = s"OracleStore[${name},${host},${user},${password}]"

  override def convertToSqlType(colType: String): String = {
    super.convertToSqlType(colType) match {
      case "text" => "varchar2(256)"
      case "bigtext" => "clob"
      case "numeric" => "number"
      case "double" => "binary_double"
      case "bigint" => "number(19)"
      case "int" => "number(10)"
      case colType: String => colType
    }
  }
  
  
  override protected def jdbcType2EtlType(sqlType: Int, size: Int, decimals: Int) : String= {
    var direct = super.jdbcType2EtlType(sqlType, size, decimals)
    
    /* In oracle, a date is really a datetime */
    if(direct == "date") 
      direct = "datetime"

    if(sqlType == 101) // BINARY_DOUBLE
      direct = "double"
    else if(sqlType == 100) // BINARY_FLOAT
      direct = "double"

    direct
  }


    
  override def getTablesFromDatabaseMetaData(schema: String) : Seq[String] = {
    withDBLocalSession { implicit session => 
      // Find tables 
      val tables: List[String] = sql"""SELECT table_name FROM all_tables
        WHERE owner = ${schema}""".map(rs => rs.string("table_name")).list.apply()
      
      logger.debug(s"Discovery from schema $schema (datastore $name) returned tables : ${tables.mkString(", ")}")

      tables
    }
  }

  
  /*
   * Create a schema if it doesn't exists
   */
  override def createSchema(schema: String) : Unit = {
    withDBLocalSession { session => 
      Try(session.execute(s"create user $schemaEscapeStart$schema$schemaEscapeEnd default tablespace $defaultTablespace"))
      session.execute(s"alter user $schemaEscapeStart$schema$schemaEscapeEnd quota unlimited on $defaultTablespace")
    }
  }

  
  override def dropSchemaIfExists(schema: String) : Unit = {
    withDBLocalSession { session => 
      val query = s"drop user $schemaEscapeStart$schema$schemaEscapeEnd cascade"
      // Avoid to fail if the schema/user already exists
      Try(session.execute(query))
    }
  }

  override def dropTableIfExists(schema: String, name: String) : Unit = {
    withDBLocalSession { session => 
      // Avoid to fail if the table already exists
      Try(session.execute(s"drop table ${sqlTablePath(schema, name)}"))
    }
  }

  
  
  override def tableExists(schema: String, name: String) : Boolean = {
    withDBReadSession { session => 
      val query = 
        if(schema == "")
          s"SELECT count(1) as NB FROM all_tables WHERE table_name = '${name}'"
        else
          s"SELECT count(1) as NB FROM all_tables WHERE owner = '${schema}' AND table_name = '${name}'"
      val exists = session.single(query)(rs => rs.int("NB"))
      if(exists.isDefined && exists.get == 1)
        true
      else 
        false
    }
  }

  override def rowIdGenerator = "cast(row_number() over (order by 1) as number(19))"
}

object OracleStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config) : OracleStore = {
    val store = new OracleStore()
    store.parse(dsXml, config)
    store
  }
}