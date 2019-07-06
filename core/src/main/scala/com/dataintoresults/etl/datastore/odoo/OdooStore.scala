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

package com.dataintoresults.etl.datastore.odoo

import scala.collection.JavaConversions._

import java.io.File
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat

import play.api.libs.json._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.impl.DataSetImpl

import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

import com.dataintoresults.util.XmlHelper._
import javax.transaction.NotSupportedException

/**
 * Odoo backend main class.
 *
 * Currently, OdooStore read every request fully and store it in memory.
 * Huge query can fail because of the allocated memory.
 */
class OdooStore extends EtlDatastore with DataStore {
		
	private val _url = EtlParameter[String](nodeAttribute = "url", configAttribute = "dw.datastore."+name+".url")
	private val _db = EtlParameter[String](nodeAttribute = "db", configAttribute = "dw.datastore."+name+".db")
	private val _user = EtlParameter[String](nodeAttribute = "user", configAttribute = "dw.datastore."+name+".user")
	private val _password = EtlParameter[String](nodeAttribute = "password", configAttribute = "dw.datastore."+name+".password")
	
	private val _tables = EtlChilds[OdooTable]()

	def url = _url.value
	def db = _db.value
	def user = _user.value
	def password = _password.value

	private var _userId: Option[Int] = None

	override def toString = s"OdooStore[${_name}]" 

  def close(): Unit = {}
	
  def tables: Seq[com.dataintoresults.etl.core.Table] = _tables
	
	def createDataSource(table: OdooTable) : DataSource = {
	  val raw = OdooHelper.read(url, db, userId, password, table.odooModel, table.odooColumns.map(_.odooField))
	  
	  val rows = raw map { row =>
	    table.odooColumns map { col : OdooColumn =>
	      val cell = row \ col.odooField 
	      col.basicType match {
	        case Column.INT => cell.asOpt[Int].getOrElse(null)
	        case Column.BIGINT => cell.asOpt[Long].getOrElse(null)
	        case Column.BIGTEXT => cell.asOpt[String].getOrElse(null)
	        case Column.TEXT => cell.asOpt[String].getOrElse(null)
	        case Column.LAZY => cell.getOrElse(null) match {
	          case null => null
	          case JsString(s) => s
	          case JsBoolean(b) => b
	          case JsNumber(n) => n
	        }
	        case _ => throw new RuntimeException(s"Type ${col.colType} not supported colum ${col.name} of table ${this.name}.${table.name}") 
	      }
	    }
	  }
		
		val dataset = new DataSetImpl(table.columns, rows)

		// We create a DataSource on top of the DataSet
		new DataSource {
		  private val iterator = dataset.rows.iterator
		  
		  def structure = dataset.columns
		  
		  def hasNext() = iterator.hasNext()
		  
		  def next() = iterator.next()
		  
		  def close() = {}
		}
	  
	}
	
	
	
	def createDataSink(gsTable: OdooTable) : DataSink = {
	  throw new RuntimeException("Writing in Odoo datastore is not supported yet (datastore ${_name})")

	  
	}
	
	
	
	private def userId : Int = {
	  	_userId match {
	  	  case Some(u) => u
	  	  case None => {
	  	    val userId = OdooHelper.authentificate(url, db, user, password)
	  	    _userId = Some(userId)
	  	    userId	  	    
	  	  }
	  	}
	}

	override def test() : Boolean = {
			true
	}
	
}


object OdooStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): DataStore = {
    val store = new OdooStore()
    store.parse(dsXml, config)
    store
  }
}