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

import play.api.Logger

import scalikejdbc._

import org.apache.commons.dbcp2.BasicDataSource;

import fr.janalyse.ssh._
import fr.janalyse.ssh.SSHPassword.string2password

import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Etl
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.ReplicateDataStore

import com.dataintoresults.etl.core.{EtlDatastore, EtlParameter, EtlChilds}
import com.dataintoresults.etl.core.EtlParameterHelper._

import com.typesafe.config.Config

import com.dataintoresults.etl.impl.EtlImpl
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.impl.ReplicateDataStoreImpl

import com.dataintoresults.util.XmlHelper._



abstract class SqlStore extends EtlDatastore with DataStore {
  private val logger: Logger = Logger(this.getClass())
 
	
	// Conversion of java.sql.Types to their bicloud counterpart (not complete)
	private val jdbcType2EtlTypeMap = Map(2014 -> "datetime", 5 -> "int", -7 -> "int", 93 -> "datetime",  
	    1  /*CHAR or ENUM in Mysql*/ -> "text",
	    6 -> "double", 2 -> "numeric", -5 -> "bigint", 12 -> "text", 7 -> "double", 91 -> "date", 3 -> "numeric",
	    -1 -> "text", 16 -> "int", -16 -> "bigtext", -9 -> "varchar", 8 -> "double", -6 -> "int", 4 -> "int", 
	    -3 /*VARBINARY*/ -> "bigtext")
	// Conversion of java.sql.Types to string
	private	val sqlTypes = classOf[java.sql.Types].getFields.toList.map(x => (x.get(null).asInstanceOf[Int], x.getName())).toMap

	
	
	def columnEscapeStart = "\"" 
	def columnEscapeEnd = "\"" 
	 	

 	
 	def sqlType : String
	def jdbcDriver : String
	def jdbcUrl : String 
	
	
	def createJdbcUrl(host: String, port: String, database: String) : String
	
	
	protected def defaultPort : String
 	protected def defaultDatabase = "nodatabase"

 	
	private val _host = EtlParameter[String](nodeAttribute="host", configAttribute="dw.datastore."+name+".host")
	private val _port = EtlParameter[String](nodeAttribute="port", configAttribute="dw.datastore."+name+".port", defaultValue=defaultPort)
  private val _database = EtlParameter[String](nodeAttribute="database", configAttribute= "dw.datastore."+name+".database", defaultValue=defaultDatabase)
  private val _user = EtlParameter[String](nodeAttribute="user", configAttribute="dw.datastore."+name+".user")
  private val _password = EtlParameter[String](nodeAttribute="password", configAttribute="dw.datastore."+name+".password")
  private val _sshUser = EtlParameter[String](nodeAttribute="sshUser", configAttribute= "dw.datastore."+name+".sshUser", defaultValue="")
  private val _sshPassword = EtlParameter[String](nodeAttribute="sshPassword", configAttribute="dw.datastore."+name+".sshPassword", defaultValue="")
  private val _sshPrivateKeyLocation = EtlParameter[String](nodeAttribute="sshPrivateKeyLocation", 
    configAttribute = "dw.datastore."+name+".sshPrivateKeyLocation", defaultValue="")
  private val _sshPrivateKeyPassphrase = EtlParameter[String](nodeAttribute="sshPrivateKeyPassphrase", 
    configAttribute = "dw.datastore."+name+".sshPrivateKeyFilePassphrase", defaultValue="")
 	private var _autoDiscovery = EtlChilds[AutoDiscovery]()

  private val _tables = EtlChilds[SqlTable]()

	def host =  _host.value
	def port =  _port.value
	def database =  _database.value
	def user =  _user.value
	def password =  _password.value
	def sshUser =  _sshUser.value
	def sshPassword =  _sshPassword.value
 	def sshPrivateKeyLocation = _sshPrivateKeyLocation.value
 	def sshPrivateKeyPassphrase = _sshPrivateKeyPassphrase.value

  def tables = _tables ++ _autoDiscoveryTables
 	private var _autoDiscoveryTables : Seq[Table] = Nil
 	
 	private var sshLocalPort : Int = _
 	
 	private var _ssh : Option[SSH] = None
 	private var _connectionPool : Option[BasicDataSource] = None; 	
 	
 	protected def connectionPool : BasicDataSource = {
 	  if(_connectionPool.isEmpty) open()
 	  _connectionPool.get
 	}
 	
 	
			
 	 	
 	
	def getTableFromDatabaseMetaData(schema: String, table: String) : Table = {
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
	
		
	def getTablesFromDatabaseMetaData(schema: String) : Seq[String] = {       
    withDB { db =>
      // Access to the metadata of the JDBC connection
			val databaseMetaData = db.conn.getMetaData();
			
			// Find tables 
			val result = new ResultSetTraversable(databaseMetaData.getTables(null, schema, null, Array("TABLE")));
			
			// Column 3 contains the table names
			val columns = result map { _.string(3) }
  			
			columns.toSeq
    }
	}
		
		
	def getModulesFromDatabaseMetaData() : Seq[String] = {       
    withDB { db =>
      // Access to the metadata of the JDBC connection
			val databaseMetaData = db.conn.getMetaData();
			
			// Find tables 
			val result = new ResultSetTraversable(databaseMetaData.getSchemas());
			
			// Column 3 contains the schema names
			val columns = result map { _.string(1) }
  			
			columns.toSeq
    }
	}
 	
 	

	def createDataSink(sqlTable: SqlTable) : DataSink = {	
	  createDataSink(sqlTable.schema, sqlTable.name, sqlTable.columns)
	}
	
	def createDataSink(schema: String, name: String, columns: Seq[Column]) : DataSink = {	
	  val db = DB(connectionPool.getConnection())
	  
    
    // Postgresql hack, if not set, fetchSize is not taken into account.
    db.conn.setAutoCommit(false)
      
    logger.info(s"SqlStore: Opening a data sink to ${this.name}.${schema}.${name}")
    
    val query = "insert into " + schema + "." + name + 
      "(" + ( columns map { columnEscapeStart + _.name + columnEscapeEnd } mkString ", ") + ")" + 
      " values (" + ( columns map { _ => "?" } mkString ", " ) +")";

    logger.info("Executing query : " + query)
       
    val stmt =  db.conn.prepareStatement(query)

    val dsName = this.name
    
		// We create a DataSource on top of the DataSet
		new DataSink {
      private var nbRows = 0L;
		  
		  def structure = columns
		  
	    def put(row: Seq[Any]) : Unit = {
		    //println("SqlDataSink.put")
		    row.zipWithIndex map {
		      case (None, i) => stmt.setObject(i+1, null)
		      case (c:scala.math.BigDecimal, i) => stmt.setObject(i+1, c.bigDecimal) 
		      case (c:java.util.Date, i) => stmt.setDate(i+1, new java.sql.Date(c.getTime)) 
		      case (c, i) => stmt.setObject(i+1, c) 
		    }
        // execute the preparedstatement insert
        stmt.addBatch()
          
        nbRows += 1
          
        if(nbRows % 1000 == 0) stmt.executeBatch()
          
        if(nbRows % 10000 == 0) System.gc()
		  }
		  
		  def close() = {		    
        logger.info(s"SqlStore: Closing a data sink to ${dsName}.${schema}.${name} (${nbRows} inserted)")
		    // Add a last execute batch in case the last put did not trigger an executeBatch
		    stmt.executeBatch()
		    stmt.close()
		    db.conn.commit()
		    db.close() 
		  }
		}
	}
 	
	private def structureFromResultSetMetaData(rs: ResultSetMetaData): Seq[Column] = {
	  val nbCol = rs.getColumnCount
	  
	  1 to nbCol map { i => 
	    val name = rs.getColumnName(i)
	    val sqlType = rs.getColumnType(i)
	    val sqlSize = rs.getScale(i)
	    val colType = jdbcType2EtlType(sqlType, sqlSize)
	    new ColumnBasic(name, colType)
	  }
	}

  /**
   * Create a query that returns no rows bu the correct metadata according to columns.
   */
  def createEmptyQuery(columns: Seq[Column]): String = {
    "select " + columns.map { c => "cast(null as " + convertToSqlType(c.colType) + ") as " + c.name}.mkString(",") + " where 0=1"
  }
	

  /**
   * Run an abritrary query, return just it it worked or not.
   */
  def execute(query: String): Boolean = {    
    withDBLocalSession { session =>
      session.execute(query)
    }
  }

	/*
	 * Create a DataSource as the result of the given query.
	 * The structure (columns) can be given (not check made) or 
	 * infered from the result set metadata if columns param is Nil.
	 */
	def createDataSource(query: String, columns: Seq[Column] = Nil): DataSource = {	
	  val db = DB(connectionPool.getConnection())
	  
	  val logQuery = query.substring(0, Math.min(query.size-1, 26)).replaceAll("\n", " ")
	  
	  logger.info(s"SqlStore: Opening request in datastore ${name} for query: ${query}")
    val session = db.readOnlySession()
    
    // Postgresql hack, if not set, fetchSize is not taken into account.
    session.connection.setAutoCommit(false)
              
    var i = 0;
    
    val stmt = session.connection.createStatement()
    // Limits the number of rows to be fetched and kept in memory to avoid OutOfmemoryException
    stmt.setFetchSize(10000)
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD)
    
    val rs = stmt.executeQuery(query)
    
    // If we don't have a structure yet, w extract the structure from the result set
    val struct = 
      if(columns.isEmpty) 
        structureFromResultSetMetaData(rs.getMetaData)
      else columns
    
		// We create a DataSource on top of the DataSet
		new DataSource {
      private var i = 0;
      private var didNext = false;
      private var _hasNext = false;
		  
		  def structure = struct
		  
		  def hasNext() : Boolean = {
        if (!didNext) {
            _hasNext = rs.next();
            didNext = true;
        }
        _hasNext;		     
		  }
		  
		  def next() = {		    
		    if(!didNext)
		      rs.next()
		    didNext = false;
        i = i +1
        val row : Seq[Any] = 1 to rs.getMetaData.getColumnCount() map { i => rs.getObject(i) match {
          case b : Boolean with Object => if(b == true) 1 else 0
          case x => x
          }
        }
        
		    // Doesn't seems to garbage well, let's help a bit
        if(i % 10000 == 0) System.gc()
        
        row
		  }
		  
		  def close() = { 
		    logger.info(s"SqlStore: Closing request in datastore ${name} for query ${logQuery}")
		    rs.close()
		    stmt.close()
		    session.close();
		    db.close() 
		  }
		}
	}

  private def sqlTablePath(schema: String, name: String): String = {
    schema match {
      case "" => name
      case null => name
      case _ => schema + "." + name
    }
  }
	
	def createDataSource(sqlTable: SqlTable) : DataSource = {	          
    val query = 
      if(sqlTable.columns.size == 0 ) // If we don't have columns   metadata, let's make a blind select and hope.
        "select * from " + sqlTablePath(sqlTable.schema, sqlTable.name);
      else
        "select " + ( sqlTable.columns map { columnEscapeStart + _.name + columnEscapeEnd } mkString ", ") + 
        " from " + sqlTablePath(sqlTable.schema, sqlTable.name);

    createDataSource(query, sqlTable.columns)
	}	
 	  
  def dropTableIfExists(schema: String, name: String) : Unit = {
    withDBLocalSession { session => 
      val query = s"drop table if exists ${sqlTablePath(schema, name)}"    			
			session.execute(query)
		}
  }
  
  
  def renameTable(schema: String, oldName: String, newName: String) : Unit = {
    withDBLocalSession { session => 
      val query = s"alter table ${sqlTablePath(schema, oldName)} rename to ${newName}";     			
			session.execute(query)
		}
  }  
  
  def tableExists(schema: String, name: String) : Boolean = {
    withDBReadSession { session => 
      val query = 
        if(schema == "")
          s"SELECT count(1) as nb FROM information_schema.tables WHERE table_name = '${name}'"
        else
          s"SELECT count(1) as nb FROM information_schema.tables WHERE table_schema = '${schema}' AND table_name = '${name}'"
			val exists = session.single(query)(rs => rs.int("nb"))
			if(exists.isDefined && exists.get == 1)
			  true
			else 
			  false
		}
  }

  def createTable(schema: String, name: String, columns: Seq[Column]) : Unit = {
    withDBLocalSession { session => 
      val query = s"create table " + schema + "." + name + "(" +
        (columns map { c => columnEscapeStart + c.name + columnEscapeEnd + " " + convertToSqlType(c.colType) } mkString ", ") + 
        ")";
			logger.info(s"Create table with definition :  $query")
 			session.execute(query)
		}
  }
  
  def createTableAs(schema: String, name: String, query: String) : Unit = {
    withDBLocalSession { session => 
      val cta = s"create table " + schema + "." + name + " as " + query;
      logger.info(s"Create table with select : $cta")
 			session.execute(cta)
		}
  }
  
  /*
   * Create a schema if it doesn't exists
   */
  def createSchema(schema: String) : Unit = {
    withDBLocalSession { session => 
      val cta = s"create schema if not exists $schema";
 			session.execute(cta)
		}
  }


  
  def convertToSqlType(colType: String): String = {
    colType match {
      case "bigtext" => "text"
      case "text" => "varchar"
      case "string" => "varchar"
      case _ => colType
    }
  }
  
  
  def md5HashFunction(columns: Seq[Column], alias : String, resultName: String) : Unit = {
    val sb = new StringBuffer(100)
    sb.append("md5(")
    sb.append(columns map { col => s" case when ${alias}.${col.name} is null then '' else cast(${alias}.${col.name} as varchar) end "  } mkString " || ")
    sb.append(") as ")
    sb.append(resultName)
  }
  
  
  def queryMergeNewOld(table: String, schema: String, columns: Seq[Column], keys: Seq[String]): String = s"""
              select 
                case when o.${columnEscapeStart}${table}_key${columnEscapeEnd} is null 
                  then (select coalesce(max(${columnEscapeStart}${table}_key${columnEscapeEnd}),0) from ${schema}.${table}_old) + row_number() over () 
                  else o.${columnEscapeStart}${table}_key${columnEscapeEnd} end as ${columnEscapeStart}${table}_key${columnEscapeEnd},  
                ${columns map { col => s"""case when n.${columnEscapeStart}update_timestamp${columnEscapeEnd} is null then o.${columnEscapeStart}${col.name}${columnEscapeEnd} else 
                n.${columnEscapeStart}${col.name}${columnEscapeEnd} end as ${columnEscapeStart}${col.name}${columnEscapeEnd},""" } mkString "" }
                coalesce(o.${columnEscapeStart}create_timestamp${columnEscapeEnd}, n.${columnEscapeStart}update_timestamp${columnEscapeEnd}) as ${columnEscapeStart}create_timestamp${columnEscapeEnd},  
                coalesce(n.${columnEscapeStart}update_timestamp${columnEscapeEnd}, o.${columnEscapeStart}update_timestamp${columnEscapeEnd}) as ${columnEscapeStart}update_timestamp${columnEscapeEnd}
              from ${schema}.${table}_old o
              full outer join (select current_timestamp as ${columnEscapeStart}update_timestamp${columnEscapeEnd}, * from ${schema}.${table}_new) n
              on ${keys map { col => s" o.${columnEscapeStart}${col}${columnEscapeEnd} = n.${columnEscapeStart}${col}${columnEscapeEnd}  " } mkString " and " }
              """
              
              
  def queryMergeNewOldNoFullJoin(table: String, schema: String, columns: Seq[Column], keys: Seq[String]): String = s"""
              select 
                case when o.${columnEscapeStart}${table}_key${columnEscapeEnd} is null 
                  then (select coalesce(max(${columnEscapeStart}${table}_key${columnEscapeEnd}),0) from ${schema}.${table}_old) + row_number() over () 
                  else o.${columnEscapeStart}${table}_key${columnEscapeEnd} end as ${columnEscapeStart}${table}_key${columnEscapeEnd},  
                ${columns map { col => s"""case when n.${columnEscapeStart}update_timestamp${columnEscapeEnd} is null then o.${columnEscapeStart}${col.name}${columnEscapeEnd} else 
                n.${columnEscapeStart}${col.name}${columnEscapeEnd} end as ${columnEscapeStart}${col.name}${columnEscapeEnd},""" } mkString "" }
                coalesce(o.${columnEscapeStart}create_timestamp${columnEscapeEnd}, n.${columnEscapeStart}update_timestamp${columnEscapeEnd}) as ${columnEscapeStart}create_timestamp${columnEscapeEnd},  
                coalesce(n.${columnEscapeStart}update_timestamp${columnEscapeEnd}, o.${columnEscapeStart}update_timestamp${columnEscapeEnd}) as ${columnEscapeStart}update_timestamp${columnEscapeEnd}
              from ${schema}.${table}_old o
              left outer join (select current_timestamp as ${columnEscapeStart}update_timestamp${columnEscapeEnd}, * from ${schema}.${table}_new) n
              on ${keys map { col => s" o.${columnEscapeStart}${col}${columnEscapeEnd} = n.${columnEscapeStart}${col}${columnEscapeEnd} " } mkString " and " }
              union all 
              select 
                case when o.${columnEscapeStart}${table}_key${columnEscapeEnd} is null 
                  then (select coalesce(max(${columnEscapeStart}${table}_key${columnEscapeEnd}),0) from ${schema}.${table}_old) + row_number() over () 
                  else o.${columnEscapeStart}${table}_key${columnEscapeEnd} end as ${columnEscapeStart}${table}_key${columnEscapeEnd},  
                ${columns map { col => s"""case when n.${columnEscapeStart}update_timestamp${columnEscapeEnd} is null then o.${columnEscapeStart}${col.name}${columnEscapeEnd} else 
                n.${columnEscapeStart}${col.name}${columnEscapeEnd} end as ${columnEscapeStart}${col.name}${columnEscapeEnd},""" } mkString "" }
                coalesce(o.${columnEscapeStart}create_timestamp${columnEscapeEnd}, n.${columnEscapeStart}update_timestamp${columnEscapeEnd}) as ${columnEscapeStart}create_timestamp${columnEscapeEnd},  
                coalesce(n.${columnEscapeStart}update_timestamp${columnEscapeEnd}, o.${columnEscapeStart}update_timestamp${columnEscapeEnd}) as ${columnEscapeStart}update_timestamp ${columnEscapeEnd}
              from ${schema}.${table}_old o
              right outer join (select current_timestamp as ${columnEscapeStart}update_timestamp${columnEscapeEnd}, * from ${schema}.${table}_new) n
              on ${keys map { col => s" o.${columnEscapeStart}${col}${columnEscapeEnd} = n.${columnEscapeStart}${col}${columnEscapeEnd} " } mkString " and " }
              where n.${columnEscapeStart}update_timestamp${columnEscapeEnd} is not null and o.${columnEscapeStart}create_timestamp${columnEscapeEnd} is null
              """
              
  /*
   * Protected functions
   */
	
	protected def jdbcType2EtlType(sqlType: Int, size: Int) : String = {
	  try  {
	    jdbcType2EtlTypeMap(sqlType)
	  } catch {
	    case e: java.util.NoSuchElementException => "text"
	  }
	}
  
  
  /*
   * Private Functions
   */
  
  /*
   * Fill _autoDiscoveryTables with schema listed in _autoDiscovery
   */
  private def discoverTables() : Unit = {
    _autoDiscoveryTables = _autoDiscovery flatMap { autoDisc =>
      getTablesFromDatabaseMetaData(autoDisc.schema) map { table =>
        getTableFromDatabaseMetaData(autoDisc.schema, table)
      }
    }
  }
  
  
  
  protected def open() : Unit = {
    if(_connectionPool.isDefined) {
      close()
    }
    
    // SSH Tunnel if needed
    if(sshPassword != "" ) {
      try {
        logger.info(s"SqlStore : Openning a SSH tunnel ${sshUser}@${host} with a password (datastore ${name})")
        _ssh = Some(SSH.apply(host, sshUser, sshPassword))
        sshLocalPort = _ssh.get.remote2Local("127.0.0.1", port.toInt)
      }
      catch {
        case e: Exception => throw new RuntimeException(s"Issue with SSH Tunneling for ${sshUser}@${host} using password", e)
      }
    }
    else if(sshUser != "" && sshPrivateKeyLocation != "") {      
      try {
        logger.info(s"SqlStore : Openning a SSH tunnel ${sshUser}@${host} with a key (datastore ${name})")
        _ssh = Some(SSH.apply(
          SSHOptions(host = host, sshUser, identities = List(SSHIdentity(sshPrivateKeyLocation, sshPrivateKeyPassphrase)))
        ))
        sshLocalPort = _ssh.get.remote2Local("127.0.0.1", port.toInt)
      }
      catch {
        case e: Exception => throw new RuntimeException(s"Issue with SSH Tunneling for ${sshUser}@${host} using private key", e)
      }
    }
        
    // Connection Pool
    val pool = new BasicDataSource();
    
    logger.info(s"SqlStore : Openning connection to : $jdbcUrl (datastore ${name})")
    
    pool.setDriverClassName(jdbcDriver);
    pool.setUsername(user);
    pool.setPassword(password);    
    pool.setUrl(_ssh match {
      case None => jdbcUrl
      case Some(_) => createJdbcUrl("127.0.0.1", sshLocalPort.toString, database)
    })
        
    _connectionPool = Some(pool) 
    
    
    // Test a connection
    try {
      withDBReadSession { session => }
    }
    catch {
      case e: Exception => _ssh match {
        case None => throw new RuntimeException(s"Issue with SQL connection ${pool.getUsername}@${pool.getUrl} with a password length of ${password.length}.", e)    
        case Some(_) => throw new RuntimeException(s"Issue with SQL connection ${pool.getUsername}@${pool.getUrl} with SSH Tunneling for ${sshUser}@${host} and a password length of ${password.length}.", e)      
      }
    }
    
    // Discover table structure if specified    
		discoverTables()
  }
  
	def close() : Unit = {
    _connectionPool foreach { 
      logger.info(s"SqlStore: Closing connection to datastore ${name}")
      _.close() 
    }
    _connectionPool = None
    _ssh foreach { _.close() }
    _ssh = None
	}
  
    
  final protected def withDB[A](f: DB => A): A = {
    using(DB(connectionPool.getConnection()))(f)
  }
  
  final protected def getDB(): DB = {
    DB(connectionPool.getConnection())
  }
  
  final protected def withDBReadSession[A](f: DBSession => A): A = {
    withDB { db =>
      db.readOnly(f)
    }
  }
  
  final protected def withDBLocalSession[A](f: DBSession => A): A = {
    withDB { db =>
      db.localTx(f)
    }
  }
  
  final protected def withPreparedStatement[A](query: String)(f: PreparedStatement => A): A = {
    withDB { db =>
      db.conn.setAutoCommit(false)
      val res = using(db.conn.prepareStatement(query))(f)
      db.commit()
      res
    }
  }
}