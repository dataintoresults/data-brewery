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
import com.dataintoresults.etl.core.Column
import com.dataintoresults.etl.core.DataSource
import com.dataintoresults.etl.core.DataSink
import com.dataintoresults.etl.impl.ColumnBasic

import com.dataintoresults.etl.core.{EtlParent, EtlChilds, EtlTable, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._

class SftpTable extends EtlTable with Table {
	private val _parent = EtlParent[SftpStore]()
	private val _columns = EtlChilds[ColumnBasic]()

 	private val _filePattern = EtlParameter[String](nodeAttribute="filePattern", configAttribute = "dw.datastore."+name+".filePattern")
 	private val _fileFormat = EtlParameter[String](nodeAttribute="fileFormat", configAttribute = "dw.datastore."+name+".fileFormat")
 	private val _csvSeparator = EtlParameter[String](nodeAttribute="csvSeparator", configAttribute = "dw.datastore."+name+".csvSeparator", defaultValue = "\t")

	def filePattern = _filePattern.value
	def fileFormat = _fileFormat.value
	def csvSeparator = _csvSeparator.value

 	def columns : Seq[Column] = _columns
	def store = _parent.get 

 	override def toString = s"SftpTable[${name},${filePattern},${fileFormat},${csvSeparator}]"

 	override def test() : Boolean = {
 		store.test(this)
 		true
 	}

	def canRead = true
	
  def read() : DataSource = store.createDataSource(this)
	
	def canWrite = false
	
	def write() : DataSink = throw new RuntimeException("You cannot write to a SftpStoreTable table.")
	
  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for SftpStoreTable")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for SftpStoreTable")

}
