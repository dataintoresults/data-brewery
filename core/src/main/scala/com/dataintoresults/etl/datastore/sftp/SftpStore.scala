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

package com.dataintoresults.etl.datastore.sftp


import fr.janalyse.ssh._
import fr.janalyse.ssh.SSHPassword.string2password

import com.dataintoresults.etl.core.DataStore
import com.dataintoresults.etl.core.Table
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.impl.DataSetImpl

import com.dataintoresults.etl.core.{EtlChilds, EtlDatastore, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class SftpStore extends EtlDatastore with DataStore {
 	private val _host = EtlParameter[String](nodeAttribute="host", configAttribute = "dw.datastore."+name+".host")
 	private var _user = EtlParameter[String](nodeAttribute="user", configAttribute = "dw.datastore."+name+".user")
 	private var _password = EtlParameter[String](nodeAttribute="password", configAttribute = "dw.datastore."+name+".password")
	private val _path = EtlParameter[String](nodeAttribute="path", configAttribute = "dw.datastore."+name+".path", defaultValue="/")

 	var _tables : Seq[SftpTable] = List[SftpTable]()

	def host = _host.value
	def user = _user.value
	def password = _password.value
	def path = _path.value 

 	def tables : Seq[Table] = _tables

 	override def toString = s"SftpStore[${name},${host},${user},${path}]"
 	
 	def test(table : SftpTable) = {
		SSH.once(host, user, password) { ssh => {
		  val sftp = new SSHFtp()(ssh);
		}
		}
 	}
 	
	def close() : Unit = {
	}
 	
	
	def createDataSource(sftpTable: SftpTable) : DataSource = {	
	  // Will all all data before transforming it in DataSet
		val datasetBuilder = Vector.newBuilder[Seq[Object]]
	  
	  // The actual SFTP grabbing
		// We don't stream currently
	  SSH.once(host, user, password) { ssh => {
		  val sftp = new SSHFtp()(ssh);
				sftp.get(path + '/' + sftpTable.filePattern) match { 
					case None => throw new RuntimeException(s"No file ${sftpTable.filePattern} found on datastore ${name}")
					case Some(txt) => txt.split("\n") foreach { r => datasetBuilder += (r.split(sftpTable.csvSeparator) map { _.trim }) }
					}
		  }
		}	  
	  
		// Create a dataset
		val dataset = new DataSetImpl(sftpTable.columns, datasetBuilder.result())

		// We create a DataSource on top of the DataSet
		new DataSource {
		  private val iterator = dataset.rows.iterator
		  
		  def structure = dataset.columns
		  
		  def hasNext() : Boolean = iterator.hasNext
		  
		  def next() = iterator.next()
		  
		  def close() = {}
		}
	}	
}

object SftpStore {
  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): DataStore = {
    val store = new SftpStore()
    store.parse(dsXml, config)
    store
  }
}