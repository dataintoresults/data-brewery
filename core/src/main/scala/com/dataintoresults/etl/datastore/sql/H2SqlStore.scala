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

import scalikejdbc._

import play.api.Logger

import org.postgresql.core.BaseConnection
import org.postgresql.copy._

import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.Etl

import com.dataintoresults.etl.impl.ColumnBasic


import com.dataintoresults.etl.impl.EtlImpl

import com.dataintoresults.etl.core.EtlParameterHelper._

class H2Store extends SqlStore {
  private val logger: Logger = Logger(this.getClass())
 	def sqlType = "h2"
	def jdbcDriver : String = "org.h2.Driver"
	def jdbcUrl : String = createJdbcUrl(host, port, database)
	
	override protected def defaultHost : Option[String] = Some("mem")
	// For H2 we allow not givin user/password parameters
  override protected def defaultUser : Option[String] = Some("dummy")
  override protected def defaultPassword : Option[String] = Some("dummy")
	
 	override def defaultDatabase = "~/h2db"
	 
	private def hostAndDatabase : String = if(host.contains(":")) s"$host$database" else s"$host:$database${connectionParametersUrl}"

	def createJdbcUrl(host: String, port: String, database: String) : String = s"jdbc:h2:$hostAndDatabase${connectionParametersUrl}"
	
 	override def toString = s"H2Store[${name},${host},${user},${password}]"

  override def convertToSqlType(colType: String): String = {
    super.convertToSqlType(colType) match {
      case "datetime" => "timestamp"
      case "double" => "double precision"
      case "numeric" => "decimal"
      case colType: String => colType
    }
  }
  
	override def defaultPort = "9092"
	
	
	
	/*
	 * We need to uppercase schema and table names.
	 */
	override def getTableFromDatabaseMetaData(schema: String, table: String) : Table = {
    withDB { db =>
			val databaseMetaData = db.conn.getMetaData();
			
			// Find columns 
			val result = new ResultSetTraversable(databaseMetaData.getColumns(null, schema, table, null));
			
			logger.debug("Request metadata from : " + schema + "." + table)
			

			
			val columns = result map { row =>
			  new ColumnBasic(row.string(4), jdbcType2EtlType(row.int(5), row.intOpt(7) getOrElse 0))  }
  			
			if(columns.isEmpty) {
			  throw new RuntimeException(s"No table ${schema}.${table} in the datastore ${this.name}. Can't get definition from the datastore.");
			}	
			
			new SqlTable(this, table, schema, columns.toSeq)  		
    }
	}
	
	override def columnEscapeStart = "\"" 
	override def columnEscapeEnd = "\"" 
	
	override def queryMergeNewOld(table: String, schema: String, columns: Seq[Column], keys: Seq[String]): String =
	   queryMergeNewOldNoFullJoin(table, schema, columns, keys)
	
	/*override def createDataSink(schema: String, name: String, columns: Seq[Column]) : DataSink = {	
	  val db = DB(connectionPool.getConnection())
	  
    
    // Postgresql hack, if not set, fetchSize is not taken into account.
    db.conn.setAutoCommit(false)
      
    logger.debug(s"SqlStore: Opening a data sink to ${name}.${schema}.${name}")
    
    val query = "insert into " + schema + "." + name + 
      "(" + ( columns map { columnEscapeStart + _.name + columnEscapeEnd } mkString ", ") + ")" + 
      " values (" + ( columns map { _ => "?" } mkString ", " ) +")";

    logger.debug("Executing query : " + query)
       
    val stmt =  db.conn.prepareStatement(query)
    
		// We create a DataSource on top of the DataSet
		new DataSink {
      private var nbRows = 0L;
		  
		  def structure = columns
		  
	    def put(row: Seq[Any]) : Unit = {
		    //println("SqlDataSink.put")
		    row.zipWithIndex map {
		      case (None, i) => stmt.setObject(i+1, null)
		      case (c:scala.math.BigDecimal, i) => stmt.setObject(i+1, c.bigDecimal) 
		      case (c, i) => stmt.setObject(i+1, c) 
		    }
		    
		    row.zipWithIndex foreach {
		      case (c, i) =>  logger.warn(c   + " - " + i)
		    }
        
        // execute the preparedstatement insert
        stmt.addBatch()
          
        nbRows += 1
          
        if(nbRows % 1 == 0) stmt.executeBatch()
          
        if(nbRows % 10000 == 0) System.gc()
		  }
		  
		  def close() = {		    
        logger.info(s"SqlStore: Closing a data sink to ${name}.${schema}.${name} (${nbRows} inserted)")
		    // Add a last execute batch in case the last put did not trigger an executeBatch
		    stmt.executeBatch()
		    stmt.close()
		    db.conn.commit()
		    db.close() 
		  }
		}
	}	*/
 	
 	
}

object H2Store {
	def fromXml(config: Config, etl : EtlImpl, dsXml : scala.xml.Node) : DataStore = {
		val store = new H2Store()
		store.parse(dsXml, config)
		store
	}
}