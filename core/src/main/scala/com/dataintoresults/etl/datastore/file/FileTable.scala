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

package com.dataintoresults.etl.datastore.file

import java.nio.file.{Files, Path, Paths}
import java.io.{InputStreamReader, BufferedInputStream, FileInputStream}
import java.io.{OutputStreamWriter, BufferedOutputStream, FileOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}


import com.dataintoresults.etl.core.{DataSource, DataSink, Table, Column}
import com.dataintoresults.etl.impl.ColumnBasic
import com.dataintoresults.etl.core.Source

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core.{EtlChilds, EtlTable, EtlParent, EtlParameter}
import com.dataintoresults.etl.core.EtlParameterHelper._
import scala.xml.Node
import com.typesafe.config.Config
import com.dataintoresults.etl.core.EtlElement
import com.dataintoresults.etl.core.EtlElementFactory

abstract class FlatFileTable extends EtlTable {

  private val _parent = EtlParent[FileStore]()

  private val _type = EtlParameter[String](nodeAttribute="type", configAttribute="dw.datastore."+store.name+"."+name+".type")

  def store = _parent.get

  def tableType = _type.value
}

object FlatFileTable extends EtlElementFactory {
  def parse(node: Node, config: Option[Config] = None, context: String = "", parent: AnyRef = null): EtlElement = {
    val name = EtlParameter[String](nodeAttribute="name")
    name.parse(node)
    val store = parent.asInstanceOf[FileStore]
    val tpe = EtlParameter[String](nodeAttribute="type", configAttribute="dw.datastore."+store.name+"."+name.value+".type")
    tpe.parse(node, config, context)

    tpe.value match {
      case "csv" => (new CSVTable()).parse(node, config, context, parent)
      case "xlsx" => (new ExcelTable()).parse(node, config, context, parent)
      case x => throw new RuntimeException(s"Cant create a table for flat file type ${x} in datastore ${store.name}")
    }
  }

  def label: String = "table"
}