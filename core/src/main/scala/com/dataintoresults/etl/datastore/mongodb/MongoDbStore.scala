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

package com.dataintoresults.etl.datastore.mongodb

import scala.collection.JavaConversions._
import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.util.Success
import scala.concurrent.ExecutionContext.Implicits.global

import java.io.File
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentLinkedQueue;

import play.api.libs.json._
import play.api.Logger

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.mongodb.scala._
import org.mongodb.scala.model.Projections._
import com.mongodb.MongoCredential._
import org.mongodb.scala.bson._

import fr.janalyse.ssh._
import fr.janalyse.ssh.SSHPassword.string2password

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.impl.DataSetImpl

import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

import com.dataintoresults.util.XmlHelper._
import javax.transaction.NotSupportedException

import com.dataintoresults.etl.core.Column.BasicType

     
     

class MongoDbStore  extends EtlDatastore with DataStore {
  private val logger: Logger = Logger(this.getClass())

	private val defaultPort = "27017"

	private val _host = EtlParameter[String](nodeAttribute="host", configAttribute = "dw.datastore."+name+".host")
	private val _database = EtlParameter[String](nodeAttribute="database", configAttribute = "dw.datastore."+name+".database")
	private val _user = EtlParameter[String](nodeAttribute="user", configAttribute = "dw.datastore."+name+".user")
	private val _password = EtlParameter[String](nodeAttribute="password", configAttribute = "dw.datastore."+name+".password")
	private val _port = EtlParameter[String](nodeAttribute="port", configAttribute = "dw.datastore."+name+".port", defaultValue=defaultPort)
 	private val _sshUser = EtlParameter[String](nodeAttribute="sshUser", configAttribute = "dw.datastore."+name+".sshUser", defaultValue="")	
 	private val _sshPassword = EtlParameter[String](nodeAttribute="sshPassword", configAttribute = "dw.datastore."+name+".sshPassword", defaultValue="")	
 	private val _sshPrivateKeyLocation = EtlParameter[String](nodeAttribute="sshPrivateKeyLocation", configAttribute = "dw.datastore."+name+".sshPrivateKeyLocation")	
 	private val _sshPrivateKeyPassphrase = EtlParameter[String](nodeAttribute="sshPrivateKeyPassphrase", configAttribute = "dw.datastore."+name+".sshPrivateKeyPassphrase")	
	private val _batchSize = EtlParameter[Int](nodeAttribute="batchSize", configAttribute = "dw.datastore."+name+".batchSize", defaultValue=100)
	private val _waitingSeconds = EtlParameter[Int](nodeAttribute="waitingSeconds", configAttribute = "dw.datastore."+name+".waitingSeconds", defaultValue=10)

	private val _tables = EtlChilds[MongoDbTable]()

	def port = _port.value
	def host = _host.value
	def database = _database.value
	def user = _user.value
	def password = _password.value
	def sshUser = _sshUser.value
	def sshPassword = _sshPassword.value
	def sshPrivateKeyLocation = _sshPrivateKeyLocation.value
	def sshPrivateKeyPassphrase = _sshPrivateKeyPassphrase.value
	def batchSize = _batchSize.value
	def waitingSeconds = _waitingSeconds.value

	def tables: Seq[Table] = _tables

 	private var sshLocalPort : Int = _ 	
 	private var _ssh : Option[SSH] = None
 	
 	private var authDb : String = "admin" 	
 		
	override def toString = s"MongoDbStore[${_name}]" 

	private def userPasswordUri = 
	  if(user != null && password != null)
	    user+":"+password+"@"
	  else if(user == null && password == null)
	    ""
	  else 
	    throw new RuntimeException(s"You need to either have no user and no password or both for MongoDb datastore ${_name}).")
	    
	def uri = s"mongodb://${userPasswordUri}${
	  if(sshUser == null) host else "127.0.0.1"}:${
	  if(sshUser == null) port else sshLocalPort}/${authDb}"
	
	def uriHiddenPassword = s"mongodb://${if(user != null)  user+":*******@" else ""}${
	  if(sshUser == null) host else "127.0.0.1"}:${
	  if(sshUser == null) port else sshLocalPort}/${authDb}${
	  if(sshUser == null)	"" else "over "+sshUser+"@"+host+":"+port}"
  
	def createDataSource(table: MongoDbTable) : DataSource = {
	  
		// We create a DataSource on top of the DataSet
		val ds = new DataSource with Observer[Document] {	
	    val queue = new ConcurrentLinkedQueue[Document]()
	    var anyMore = Promise[Boolean]().success(true)
	    var requesting : Long = 0L
	    
	    // MongoDB observer part
      var seen: Long = 0
      var subscription: Option[Subscription] = None
		  
      open()
	    
	    logger.info("MongoDb: Connecting to MongoDb URI : " + uriHiddenPassword)
  	  val client: MongoClient = MongoClient(uri)
	    logger.info("MongoDb: Setting database to " + database)
  	  val db: MongoDatabase = client.getDatabase(database)
	    logger.info("MongoDb: Getting collection " + table.name)
  	  val collection: MongoCollection[Document] = db.getCollection(table.name)  	  
	    collection.find().projection(include(table.columns.map({c : Column => c.name}):_*)).subscribe(this)
      
      override def onSubscribe(subscription: Subscription): Unit = {
        this.subscription = Some(subscription)
      }
      
      override def onNext(row: Document): Unit = this.synchronized {
        queue.add(row)
        requesting -= 1;
        if(!anyMore.isCompleted) 
          anyMore.success(true)
      }
      
      override def onError(e: Throwable): Unit = throw new RuntimeException(e)
    
      override def onComplete(): Unit = this.synchronized { 
        if(anyMore.isCompleted) 
          anyMore = Promise[Boolean]().success(false)
        else
          anyMore.success(false)
      }
      
      // DataSource part      	  
		  def structure = table.columns
		  
		  def hasNext() : Boolean = {
		    (this.synchronized {
		      // Either there is already available rows
  		    if(queue.size() > 0) 
  		      Some(true)
  		    // Or it is the end already
  		    else if(anyMore.isCompleted && anyMore.future.value == Some(Success(false)))
  		      Some(false)
  		    // Or some are requested in the pipe
  		    else if(requesting > 0)
  		      None
  		    else {
  		      if(requesting == 0L) {
          	  requesting = batchSize;
              logger.info(s"MongoDB: Requesting ${batchSize} more rows for ${_name}.${table.name}")
              // Running the request in another thread, to avoid reentry .
              Future { subscription.get.request(batchSize) }
            }
      		  anyMore = Promise[Boolean]()	
      		  None
  		    }
		    } ) match { // This part is no longer synchronized
		      case Some(res) => res
		      case None => {
		        try {
		          Await.result(anyMore.future, 15 seconds)
		        } catch {
		          case e: java.util.concurrent.TimeoutException =>
		            throw new RuntimeException(s"MongoDB: No response from the server ${uriHiddenPassword} for ${waitingSeconds} second for table ${_name}.${table.name}.")
		        }
		      }
		    }
		  }
		  
		  def next() = this.synchronized {		
		    if(queue.size() > 0) 
		      documentToRow(queue.poll())
		    else 
		      throw new RuntimeException("Trying to access mongodb data when none is present.");
		  }
		  
		  def close() = {
		    logger.info(s"MongoDB: Closing connection for ${_name}.${table.name}")
		    subscription.get.unsubscribe()
		    client.close()		    
		  }
		  
		  def documentToRow(document : Document): Seq[Any] = {
		    structure map { col =>
		      if(!document.contains(col.name)) null
		      else  {
		        val bson = document.apply(col.name)
		        col.basicType match {
		          case Column.BIGINT => 
		            if(bson.isInt64()) bson.asInt64().getValue
		            else throw new RuntimeException(s"Expecting BIGINT but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		          case Column.BOOLEAN => 
		            if(bson.isBoolean()) bson.asBoolean().getValue
		            else throw new RuntimeException(s"Expecting BOOLEAN but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		          case Column.DATE => 
		            if(bson.isDateTime()) new java.util.Date(bson.asDateTime().getValue)
		            else throw new RuntimeException(s"Expecting DATE but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		          case Column.INT => 
		            if(bson.isInt32()) bson.asInt32().getValue
		            else throw new RuntimeException(s"Expecting INT but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		          case Column.LAZY => throw new RuntimeException(s"lazy type is not supported for MongoDb datasources location ${_name}.${table.name}")
		          case Column.NUMERIC => 
		            if(bson.isNumber()) bson.asNumber().decimal128Value().bigDecimalValue()
		            else throw new RuntimeException(s"Expecting NUMERIC but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		          case Column.TEXT => 
		            if(bson.isString()) bson.asString().getValue
		            else if(bson.isDocument()) bson.asDocument().toJson()
		            else throw new RuntimeException(s"Expecting STRING but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		          case Column.DATETIME => 
		            if(bson.isDateTime()) new java.util.Date(bson.asDateTime().getValue)
		            else throw new RuntimeException(s"Expecting TIMESTAMP but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		          case Column.BIGTEXT => 
		            if(bson.isString()) bson.asString().getValue
		            else if(bson.isDocument()) bson.asDocument().toJson()
		            else throw new RuntimeException(s"Expecting STRING but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		          case Column.VARIANT => 
		            if(bson.isString()) bson.asString().getValue
		            else if(bson.isDocument()) bson.asDocument().toJson()
		            else throw new RuntimeException(s"Expecting STRING but getting ${bson.getBsonType.name()} for ${_name}.${table.name}.${col.name}")
		        }
		      }
		    }
		  }
	  } 
    
    
	  
	  ds	  
	}
	
	
	
	def createDataSink(gsTable: MongoDbTable) : DataSink = {
	  throw new RuntimeException("Writing in MongoDb datastore is not supported yet (datastore ${_name})")

	  
	}
	
	

	override def test() : Boolean = {
			true
	}
	
	protected def open() : Unit = {    
    // SSH Tunnel if needed
	  if(_ssh.isEmpty)
      if(sshPassword != null ) {
        try {
          logger.info(s"SqlStore : Openning a SSH tunnel ${sshUser}@${host} with a password (datastore ${_name})")
          _ssh = Some(SSH.apply(host, sshUser, sshPassword))
          sshLocalPort = _ssh.get.remote2Local("127.0.0.1", port.toInt)
        }
        catch {
          case e: Exception => throw new RuntimeException(s"Issue with SSH Tunneling for ${sshUser}@${host} using password", e)
        }
      }
      else if(sshUser != null && sshPrivateKeyLocation != "") {      
        try {
          logger.info(s"MongoDb: Openning a SSH tunnel ${sshUser}@${host} with a key (datastore ${_name})")
          _ssh = Some(SSH.apply(
            SSHOptions(host = host, sshUser, identities = List(SSHIdentity(sshPrivateKeyLocation, sshPrivateKeyPassphrase)))
          ))
          sshLocalPort = _ssh.get.remote2Local("127.0.0.1", port.toInt)
        }
        catch {
          case e: Exception => throw new RuntimeException(s"Issue with SSH Tunneling for ${sshUser}@${host} using private key", e)
        }
      }
        
  }
	
  def close(): Unit = {    
      
    // SSH Tunnel if needed
    _ssh foreach { 
      logger.info(s"MongoDb: Closing connection to datastore ${_name}")
      _.close() 
    }
    _ssh = None
  }
}


object MongoDbStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): DataStore = {
    val store = new MongoDbStore()
    store.parse(dsXml, config)
    store
  }
}