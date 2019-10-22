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

package com.dataintoresults.etl.impl

import scala.reflect.runtime.universe._
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.dataintoresults.etl.core.{Column, EtlElement, EtlParameter, Table}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.util.XmlHelper._
import fastparse._
import scala.util.{Try, Success, Failure}
import play.api.libs.json.{Json, JsValue}
import com.typesafe.config.Config

object ParameterParser {
	def parse(parameter: String, conf: Config): String = {
		val pattern = """\$\{conf.([0-9a-zA-Z-_/.]+)}""".r
		parameter match {
			case pattern(confKey) => conf.getString(confKey)
			case _ => parameter
		}
	}
}