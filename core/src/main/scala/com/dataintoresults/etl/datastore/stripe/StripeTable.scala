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

package com.dataintoresults.etl.datastore.stripe

import java.io.File
import java.util.Arrays

import scala.collection.JavaConversions._

import com.dataintoresults.etl.core.{Column, DataSink, DataSource, DataStore, EtlChilds, EtlDatastore, EtlParameter, EtlParent, EtlTable, Table}
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.impl.ColumnBasic


class StripeTable extends EtlTable with Table {

  private val _columns  = EtlChilds[StripeColumn]()

  private val _endpoint  = EtlParameter[String](nodeAttribute = "endpoint")
  private val _root  = EtlParameter[String](nodeAttribute = "root")

  def endpoint = _endpoint.value
  def root = _root.value

  def columns = _columns

  def stripeColumns = _columns

  val _parent = EtlParent[StripeStore]()

  def store = _parent.get


  override def toString = s"StripeTable[${name},$endpoint,(${columnsAsString()})]"

  def canRead = true

  def read() : DataSource = store.createDataSource(this)

  def canWrite = false

  def write() : DataSink = throw new RuntimeException("You cannot write to a Stripe table.")

  def dropTableIfExists() : Table = throw new RuntimeException("dropTableIfExists not implemented for Stripe")

  def createTable() : Table = throw new RuntimeException("createTable not implemented for Stripe")

  override def test() : Boolean = {
    true
  }

  private def columnsAsString() : String = {
    columns map { _.name } mkString ", "
  }
}
