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

import scala.util.Try
import scala.xml._
import scala.annotation.implicitNotFound
import com.typesafe.config.Config
import com.dataintoresults.util.XmlHelper._
import com.dataintoresults.util.ReflectionHelper._

import com.dataintoresults.etl.impl.source.EtlSource

import scala.collection.JavaConverters._

import scala.reflect.runtime.universe._


@implicitNotFound("ETL Parameter should be of type String or Int.")
sealed trait EtlParameterType[T] {
  def parse(str: String): T
}


object EtlParameterHelper {
  implicit def someGeneric[T](value: T) = Some(value) 

  implicit val stringToString = new EtlParameterType[String] {
    def parse(str: String) = str
  }

  implicit val stringToInt = new EtlParameterType[Int] {
    def parse(str: String) = str.toInt
  }

  def parse[T: EtlParameterType](str: String): T = {
    implicitly[EtlParameterType[T]].parse(str)
  }
}



sealed trait EtlParameterSource

object EtlParameterSource {
  case object Default extends EtlParameterSource
  case object Xml extends EtlParameterSource
  case object Config extends EtlParameterSource
  case object Given extends EtlParameterSource
  case object CData extends EtlParameterSource
}

object EtlParameter {
  def apply[T: EtlParameterType](
    nodeAttribute: Option[String] = None, 
    configAttribute:  => Option[String] =  None,
    parameterName: Option[String] = None,
    defaultValue: Option[T] = None,
    givenValue: Option[T] = None,
    cdata: Boolean = false) = {
      new EtlParameter[T](
        nodeAttribute = nodeAttribute, 
        configAttribute = configAttribute,
        defaultValue = defaultValue,
        givenValue = givenValue,
        cdata = cdata)
    }
}

class EtlParameter[T: EtlParameterType](
  nodeAttribute: Option[String] = None, 
  configAttribute: => Option[String] =None,
  parameterName: Option[String] = None,
  defaultValue: Option[T] = None,
  givenValue: Option[T] = None,
  cdata: Boolean = false) {
  
  private var _value: Option[T] = givenValue
  var source: Option[EtlParameterSource] = givenValue match {
    case Some(_) => Some(EtlParameterSource.Given)
    case None => None
  }  

  def name(): String =
    Seq(parameterName, nodeAttribute, configAttribute).collectFirst{ case Some(d) => d }
      .getOrElse { throw new RuntimeException(s"No name for this parameter") }

  def value(): T = _value.getOrElse(throw new RuntimeException(s"No value for attribut ${name}"))

  def set(v: T): Unit = _value = Some(v)
  def value(v: T): Unit = _value = Some(v)
  def value_=(v: T): Unit = _value = Some(v)

  def addToXmlNode(node: Node): Node = {
    node match {
      case elem: Elem  => elem % Attribute(None, name(), Text(value.toString), Null) 
      case other => throw new RuntimeException(s"Should have been a xml.Elem")
    }
  }

  def help(): String = "Attribute $nodeAttribute can be set with :\n" +
    (nodeAttribute match {
      case None => ""
      case Some(attribute) => " - XML attribute $attribute\n" 
    }) + 
    (configAttribute match {
      case None => ""
      case Some(attribute) => " - configuration file $attribute\n" 
    }) + 
    (cdata match {
      case false => ""
      case true => " - XML element content enclosed in <![CDATA[ and ]]>\n" 
    }) + 
    (defaultValue match {
      case None => " - it has no default value"
      case Some(value) => " - default value : $value\n" 
    }) 

  def parse(node: Option[Node] = None, config: Option[Config] = None, context: String = ""): Unit = {
    val nodeValue: Option[T] = (node, nodeAttribute) match {
      case (_, None) => None
      case (None, _) => None
      case (Some(n), Some(a)) => (n \@? a).map(EtlParameterHelper.parse[T](_))
    } 
  
    val configValue: Option[T] = (config, configAttribute) match {
      case (_, None) => None
      case (None, _) => None
      case (Some(c), Some(a)) => 
        c.hasPath(a) match {
          case false => None
          case true => {
            Some(EtlParameterHelper.parse[T](c.getString(a)))
          }
        }
    }

    val cdataValue = 
      if(cdata && node.isDefined && node.get.text.trim.length > 0) { 
        Some(EtlParameterHelper.parse[T](node.get.text.trim))
      }
      else None
    
    source = Seq(
      givenValue.map[EtlParameterSource](_ => EtlParameterSource.Given), 
      configValue.map[EtlParameterSource](_ => EtlParameterSource.Config), 
      cdataValue.map[EtlParameterSource](_ => EtlParameterSource.CData),
      nodeValue.map[EtlParameterSource](_ => EtlParameterSource.Xml), 
      defaultValue.map[EtlParameterSource](_ => EtlParameterSource.Default)).collectFirst{ case Some(d) => d }

    _value = Seq(givenValue, configValue, cdataValue, nodeValue, defaultValue).collectFirst{ case Some(d) => d }

    if(_value == None)
      throw new RuntimeException(s"No value for parameter $nodeAttribute $context") 
  }

}


import EtlParameterHelper._



abstract class EtlDatastore extends EtlElement("datastore") {

  protected val _name = EtlParameter[String](nodeAttribute = "name")
  protected val _type = EtlParameter[String](nodeAttribute = "type")

  def name: String = _name.value
  def name_(value: String): Unit = _name.value(value)
  def datastoreType: String = _type.value
}

class EtlColumn extends EtlElement("column") {
  protected val _name = EtlParameter[String](nodeAttribute = "name")
  def name: String = _name.value

  protected val _type = EtlParameter[String](nodeAttribute = "type")
  def colType: String = _type.value
}

abstract class EtlTable extends EtlElement("table") with Table {
  protected val _name = EtlParameter[String](nodeAttribute = "name")
  def name: String = _name.value

	protected val _source = EtlOptionalChild[EtlSource]()

  override def hasSource() : Boolean = _source.isDefined
  
  override  def source : Source = {
	  if(!hasSource)
	    throw new RuntimeException("There is no source for this table")
	  else 
	    _source.get
	}
}

class EtlDataWarehouse extends EtlElement("dataWarehouse") {
  val stores = EtlChilds[EtlDatastore]()
  override def toString() =  stores.map(_.toString ).mkString("\n")
}




object EtlChilds {
  def apply[T <: EtlElement: TypeTag](): EtlChilds[T] = new EtlChilds[T]()
}



class EtlChilds[T <: EtlElement : TypeTag](minChilds: Int = 0, maxChilds:Int = Int.MaxValue) extends Seq[T] {
  private var childs : Seq[T] = Nil

  def parse(node: Option[Node] = None, config: Option[Config] = None, context: String = "", parent: AnyRef = null): Unit = {
    val generateByInstanciation = !isAbstract[T]
    val factory: Option[EtlElementFactory] =
      if (generateByInstanciation)
        None
      else
        Some(getCompanion[T].asInstanceOf[EtlElementFactory])
    val label = factory.getOrElse(createInstance[T]).label

    childs = node.toSeq.flatMap(n => n.descendant)
      .filter(_.label == label)
      .map { n =>
        factory.getOrElse(createInstance[T]).parse(node = n, parent = parent).asInstanceOf[T]
      }

    if(childs.size < minChilds) {
      throw new RuntimeException(s"There should be at least $minChilds elements of type $label, there is only ${childs.size} given.")
    }
    
    if(childs.size > maxChilds) {
      throw new RuntimeException(s"There should be at most $minChilds elements of type $label, there is ${childs.size} given.")
    }
  }

  def toXml(): NodeSeq = {
    childs.map { c => c.toXml()}
  }

  def iterator: Iterator[T] = childs.iterator

  def apply(idx: Int) : T = childs(idx)

  def set(c : Seq[T]) : Unit = childs = c

  def length : Int = childs.length
}

/**
 * Specific case when we want one and only one child of this type.
 */
case class EtlChild[T <: EtlElement : TypeTag]() extends EtlChilds[T](1, 1) {
  def value = head
}

/**
 * Specific case when we want zero or one child of this type.
 */
case class EtlOptionalChild[T <: EtlElement : TypeTag]() extends EtlChilds[T](0, 1) {
  def isDefined = size == 1
  def get = head
  def value = head

  def toOption: Option[T] = if(isDefined) Some(get) else None

}




object EtlParent {
  def apply[T  : TypeTag](): EtlParent[T] = new EtlParent[T]()
}

class EtlParent[T : TypeTag]()  {
  private var _parent : T = _

  def parse(node: Option[Node] = None, config: Option[Config] = None, context: String = "", parent: AnyRef = null): Unit = {
    this._parent = parent.asInstanceOf[T]
  }

  def hasParent = _parent !=  null
  def get = _parent

  def set(p:T):Unit  = _parent = p

  def toXml(): NodeSeq = Seq()

}


