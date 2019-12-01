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

import java.util.Date
import java.text.SimpleDateFormat

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.typesafe.config.Config

import scalikejdbc._

import play.api.Logger

import org.postgresql.core.BaseConnection
import org.postgresql.copy._

import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.Etl

import com.dataintoresults.etl.core.EtlParameterHelper._


import com.dataintoresults.etl.impl.EtlImpl
import java.time.ZoneId
import java.time.ZoneOffset


class PostgreSqlStore extends SqlStore {
  private val logger: Logger = Logger(this.getClass())
 	def sqlType = "postgresql"
	def jdbcDriver : String = "org.postgresql.Driver"
	def jdbcUrl : String = createJdbcUrl(host, port, database)
	
	override def defaultConnectionParameters: String = "sslmode=prefer&sslfactory=org.postgresql.ssl.NonValidatingFactory"
	
	def createJdbcUrl(host: String, port: String, database: String) : String = s"jdbc:postgresql://${host}:${port}/${database}${connectionParametersUrl}"
	
 	override def toString = s"PostgreSqlStore[${name},${host},${user},${password}]"

  override def convertToSqlType(colType: String): String = {
    super.convertToSqlType(colType) match {
      case "datetime" => "timestamp"
      case "double" => "double precision"
      case colType: String => colType
    }
  }
  
	override def defaultPort = "5432"
	
	private trait Parser {
	  def apply(a: Any) : String
	}
	
	private object parseBasic extends Parser {
	  def apply(a: Any) : String = a match {
	    case null => ""
	    case None => ""
	    case _ => a.toString
	  }
	}
	private object parseString extends Parser {
	  def apply(a: Any) : String = a match {
	    case null => ""
	    case None => ""
	    case a : String => "\"" + a.replaceAll("\"", "\"\"") + "\""
	    case a : Any => "\"" + a.toString.replaceAll("\"", "\"\"") + "\""
	  }
	}
	
	private object parseDate extends Parser {
	  val fmt2 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	  def apply(a: Any) : String = a match {
	    case null => ""
	    case None => ""
	    case a : LocalDateTime => a.format(fmt2)
	    case a : LocalDate => a.format(fmt2)
	    case a : Any => ""
	  }
	}
	
	private object parseDateTime extends Parser {
	  val fmt2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	  val fmt3 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	  def apply(a: Any) : String = a match {
	    case null => ""
	    case None => ""
	    case a : LocalDateTime => a.format(fmt2) + "Z"
	    case a : LocalDate => a.format(fmt3) + " 00:00:00.000Z"
	    case a : Date => throw new RuntimeException("not expected")
	    case a : Any => ""
	  }
	}
	
 	
	override  def tableExists(schema: String, name: String) : Boolean = {
    withDBReadSession { session => 
      val query = 
        if(schema == "")
          s"SELECT count(1) as nb FROM information_schema.tables WHERE table_schema in (select unnest(current_schemas(false))) AND table_name = '${name}'"
        else
          s"SELECT count(1) as nb FROM information_schema.tables WHERE table_schema = '${schema}' AND table_name = '${name}'"
			val exists = session.single(query)(rs => rs.int("nb"))
			if(exists.isDefined && exists.get == 1)
			  true
			else 
			  false
		}
  }

	/*
	 * Use COPY command from Postgresql to be faster.
	 * Postgresql doesn't batch well
	 */
	override def createDataSink(schema: String, name: String, columns: Seq[Column]) : DataSink = {	
    val db = getDB()
    val dsName = this.name
	    
    
    logger.info(s"PostgreSqlStore : Start batch copy to ${dsName}.${schema}.${name})")
    
    
    val copyManager: CopyManager = new CopyManager(db.conn.unwrap(classOf[BaseConnection]));
      
    val parsers : Seq[Parser] = columns map { c => c.basicType match {
      case Column.BIGTEXT => parseString
      case Column.TEXT => parseString
      case Column.DATE => parseDate
      case Column.DATETIME => parseDateTime
      case _  => parseBasic        
    }}
    
    val copyIn : CopyIn = copyManager.copyIn("COPY " + schema + "." + name + 
      "(" + ( columns map { columnEscapeStart + _.name + columnEscapeEnd } mkString ", ") + " ) " + 
      " FROM STDIN WITH (FORMAT csv, DELIMITER '|' ,  QUOTE '\"',  ESCAPE '\"', ENCODING 'utf-8')");
    
		// We create a DataSource on top of the DataSet
		new DataSink {
      private var i = 0;
		  
		  def structure = columns
		  
	    def put(row: Seq[Any]) : Unit = {
		    //println("PgDataSink.put")
        val rowStr = (row zip parsers) map { case (r, p) => p(r) }
          
        val str = (rowStr mkString "|") +  "\n"
                
        //logger.info(str)
        val bytes = str.getBytes("UTF8")
        copyIn.writeToCopy(bytes, 0, bytes.length);
		  }
		  
		  def close() = { 
		    //println("PgDataSink.close")
		    try {
          val end = copyIn.endCopy();
		    }
		    catch {
		      case e: Exception => {}
		    }
        
        if(copyIn.isActive()) copyIn.endCopy()

        logger.info(s"PostgreSql: End batch copy to ${dsName}.${schema}.${name} (${copyIn.getHandledRowCount()} rows)")
        
		    db.close() 
		  }
		}
	}
}

object PostgreSqlStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): PostgreSqlStore = {
    val store = new PostgreSqlStore()
    store.parse(dsXml, config)
    store
  }
}
