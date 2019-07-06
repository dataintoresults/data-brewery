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

package com.dataintoresults.etl.core

import com.dataintoresults.util.ReflectionHelper.createInstance

import scala.xml.{Attribute, Elem, Node, Null}
import com.typesafe.config.Config

import scala.reflect.api.Symbols
import scala.reflect.runtime.universe._
import com.dataintoresults.util.XmlHelper._

abstract class EtlElement(_label: String) extends EtlElementFactory  {
  def label = _label


  def parse(node: Node, config: Option[Config] = None, context: String = "", parent: AnyRef = null): EtlElement = {
    val rm = scala.reflect.runtime.currentMirror
    // The reverse is to have higher base classes first
    rm.classSymbol(this.getClass).baseClasses.reverse.foreach { case clazz: ClassSymbol =>
      val tpe = clazz.toType
      //println(tpe)
      val mirror = rm.reflect(this)
      // First parse the parent if any
      tpe.members.foreach { case term : TermSymbol =>
        val tpe = term.info
        if(tpe <:< typeOf[EtlParent[_]]) {
          val fieldMirror = mirror.reflectField(term)
          val p = fieldMirror.get.asInstanceOf[EtlParent[EtlElement]]
          p.parse(Some(node), config, parent=parent)
        }
        case _ => {} // Like trait defined in classes
      }
      // Then other properties
      tpe.members.foreach { case term : TermSymbol =>
        val tpe = term.info
        if(tpe <:< typeOf[EtlParameter[String]]) {
          val fieldMirror = mirror.reflectField(term)
          val p = fieldMirror.get.asInstanceOf[EtlParameter[String]]
          p.parse(Some(node), config)
        }
        else if(tpe <:< typeOf[EtlParameter[Int]]) {
          val fieldMirror = mirror.reflectField(term)
          val p = fieldMirror.get.asInstanceOf[EtlParameter[Int]]
          p.parse(Some(node), config)
        }
        else if(tpe <:< typeOf[EtlChilds[_]]) {
          val fieldMirror = mirror.reflectField(term)
          val p = fieldMirror.get.asInstanceOf[EtlChilds[EtlElement]]
          p.parse(Some(node), config, parent=this)
        }
        case _ => {} // Like trait defined in classes
      }
    }

    this
  }

  def toXml(): Elem = {
    var elem = <xml/>.copy(label = label)

    val rm = scala.reflect.runtime.currentMirror
    rm.classSymbol(this.getClass).baseClasses.foreach { case clazz: ClassSymbol =>
      val tpe = clazz.toType
      val mirror = rm.reflect(this)
      tpe.members.foreach { case term : TermSymbol =>
        val tpe = term.info
        if(tpe <:< typeOf[EtlParameter[String]]) {
          val fieldMirror = mirror.reflectField(term)
          val p = fieldMirror.get.asInstanceOf[EtlParameter[String]]
          if(p.source == Some(EtlParameterSource.Xml) || p.source == Some(EtlParameterSource.Given))
            elem = elem % Attribute(null, p.name,  p.value, Null)
        }
        else if(tpe <:< typeOf[EtlParameter[Int]]) {
          val fieldMirror = mirror.reflectField(term)
          val p = fieldMirror.get.asInstanceOf[EtlParameter[Int]]
          if(p.source == Some(EtlParameterSource.Xml) || p.source == Some(EtlParameterSource.Given))
            elem = elem % Attribute(null, p.name,  p.value.toString, Null)
        }
        else if(tpe <:< typeOf[EtlChilds[_]]) {
          val fieldMirror = mirror.reflectField(term)
          val p = fieldMirror.get.asInstanceOf[EtlChilds[EtlElement]]
          elem = elem.withChilds(p.toXml())
        }
      }
    }
    elem
  }
}