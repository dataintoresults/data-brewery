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



class EtlProcess extends EtlElementWithName(EtlProcess.label) with Process {
	private val _email = EtlParameter[String](nodeAttribute="email", configAttribute="dw.process."+name+".email", defaultValue = "")
	private val _emailWhen = EtlParameter[String](nodeAttribute="emailWhen", configAttribute="dw.process."+name+".emailWhen", defaultValue = "error,warning,success")
	private val _tasks = EtlChilds[EtlTask]()
  
  override def emails: Seq[String] = _email.value().split(",").filterNot(_.isEmpty())
  override def emailWhen: Seq[ProcessResult.ProcessStatus] = _emailWhen.value().split(",").filterNot(_.isEmpty()).map(
    _ match {
      case "success" => ProcessResult.Success
      case "warning" => ProcessResult.Warning
      case "error" => ProcessResult.Error
    }
  )

  def tasks: Seq[EtlTask] = _tasks

}

object EtlProcess   {
  def label: String = "process"

  def fromXml(dsXml: scala.xml.Node, config: com.typesafe.config.Config): EtlProcess = {
    val process = new EtlProcess()
    process.parse(dsXml, config)
    process
  }
}