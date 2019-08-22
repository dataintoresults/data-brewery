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

package com.dataintoresults.util

import scala.xml.{Elem, Node}
import scala.xml.NodeSeq

object XmlHelper {
  implicit class XmlBetterNode(node : Node) {
    def \@? (attribute: String) : Option[String] = {
      node.attribute(attribute) flatMap (_.headOption) map (_.text)
    }

  }

  implicit class XmlBetterElem(elem : Elem) {
    def withChild(newChild: Node) = {
      elem.copy(child = elem.child ++ newChild)
    }

    def withChilds(newChilds: Seq[Node]) = {
      elem.copy(child = elem.child ++ newChilds)
    }
/*
    def \* (label: String): Seq[Elem] = {
      elem.child flatMap {
        case e: Elem if e.label == label => e
        case e: Elem if e.label == "include" => e \ label
        case _ => None
      }
    }*/
  }
}