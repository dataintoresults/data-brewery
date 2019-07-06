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

package com.dataintoresults.etl.impl.source


import scala.util.{Try, Success, Failure}
import scala.xml.{Attribute, Elem, Node, Null}
import com.typesafe.config.Config

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core._
import com.dataintoresults.etl.core.EtlParameter._
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.etl.impl._
import com.dataintoresults.util.Using._


abstract class EtlCondition extends EtlElement(EtlCondition.label) {
	private val _type = EtlParameter[String](nodeAttribute="type")

	def conditionType = _type.get

	def isValidated(etl: EtlImpl): Boolean
}

class ConditionQuery extends EtlCondition { 
	private val _onError = EtlParameter[String](nodeAttribute="onError", defaultValue="false")
	private val _datastore = EtlParameter[String](nodeAttribute="datastore")
	private val _query = EtlParameter[String](nodeAttribute="query", cdata=true)

	def onError: Boolean = _onError.value match  {
		case "true" => true
		case "false" => false
		case _ => throw new RuntimeException(s"There is a onError attribute with value ${_onError.value}. This is unexpected.")
	}

	def query = _query.value
	def datastore = _datastore.value

	override def isValidated(etl: EtlImpl): Boolean = {
		val ds = etl.findDataStore(datastore)

		if(!ds.isInstanceOf[SqlStore]) 
			throw new RuntimeException(s"If you want to run a SQL query for a condition in the datastore ${datastore} it should be SQL compliant.")

		val sqlDs = ds.asInstanceOf[SqlStore]

		Try {
			using(sqlDs.createDataSource(query)) { ds =>
				val n: Any = ds.next()(0)
				n match {
					case b : Boolean => b
					case i : Int => i > 0
					case s : String => "true".equals(s)
				}
			}
		} match {
			case Success(b: Boolean) => b
			case Failure(_) => onError
		}
	}
}



object EtlCondition extends EtlElementFactory {
	final val label = "condition"
	
  def parse(node: Node, config: Option[Config] = None, context: String = "", parent: AnyRef = null): EtlCondition = {
    val tpe = (node \@? "type")
      .getOrElse(throw new RuntimeException(s"No value for parameter type for source element. $context"))
    val cond = tpe match {
      case "query" => new ConditionQuery()
    } 

    cond.parse(node, config, context).asInstanceOf[EtlCondition]
  }
}