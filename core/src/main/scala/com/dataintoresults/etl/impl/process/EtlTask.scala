/*******************************************************************************
 *
 * Copyright (C) 2019 by Obsidian SAS : https://dataintoresults.com/
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

package com.dataintoresults.etl.impl.process


import scala.xml.{Attribute, Elem, Node, Null}
import com.typesafe.config.Config

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core._
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.etl.impl._


object EtlTask  extends EtlElementFactory {  
	def label: String = "task"

	
	
  def parse(node: Node, config: Option[Config] = None, context: String = "", parent: AnyRef = null): EtlTask = {
		val taskType = 
			if((node \@? "module").isDefined) new EtlTaskModule()
			else if((node \@? "datastore").isDefined) new EtlTaskDatastore()
			else if((node \@? "shellCommand").isDefined) new EtlTaskShell()
      else throw new RuntimeException(s"Can't find the type of task in process ${parent.asInstanceOf[EtlProcess].name}. $context")

		taskType.parse(node, config, context).asInstanceOf[EtlTask]
  }
}
 
abstract class EtlTask extends EtlElement(EtlTask.label) with Task {
	private val _process = EtlParent[EtlProcess]()
	private val _onError = EtlParameter[String](nodeAttribute = "onError", defaultValue = "error")
	protected val _name = EtlParameter[String](nodeAttribute = "name", defaultValue = "")

	
	def dependencies: Seq[String] = Seq()

	def process = _process.get

	def onError: Task.OnError = _onError.value match {
		case "error" => Task.OnErrorError
		case "warning" => Task.OnErrorWarning
		case "success" => Task.OnErrorSuccess
		case err => throw new RuntimeException(s"Error in the onError attribut for task in process ${process.name}. It is $err and should be success, warning or error.")
	}

	def module: String = throw new RuntimeException(s"Trying to acess to a module from a task that doesn't contain any in process ${process.name}")

	def datastore: String = throw new RuntimeException(s"Trying to acess to a datastore from a task that doesn't contain any in process ${process.name}")

	def shellCommand: String = throw new RuntimeException(s"Trying to acess to a shell command from a task that doesn't contain any in process ${process.name}")

	def shellParameters: Seq[String] = throw new RuntimeException(s"Trying to acess to a shell command from a task that doesn't contain any in process ${process.name}")
}

class EtlTaskModule extends EtlTask {
	private val _module = EtlParameter[String](nodeAttribute = "module")

	def name = if(_name.value == "") module else _name.value

	override def module = _module.value 

	def taskType = Task.MODULE
}


class EtlTaskDatastore extends EtlTask {
	private val _datastore = EtlParameter[String](nodeAttribute = "datastore")

	def name = if(_name.value == "") datastore else _name.value

	override def datastore = _datastore.value 

	def taskType = Task.DATASTORE
}


class EtlTaskParameters extends EtlElement("parameter") {
	private val _value = EtlParameter[String](nodeAttribute = "value", cdata = true)

	def value = _value.value 
}


class EtlTaskShell extends EtlTask {
	private val _shellCommand = EtlParameter[String](nodeAttribute = "shellCommand")
	private val _parameters = EtlChilds[EtlTaskParameters]

	def name = if(_name.value == "") shellCommand else _name.value

	override def shellCommand = _shellCommand.value 
	override def shellParameters = _parameters.map(p => p.value)

	def taskType = Task.SHELL
}
