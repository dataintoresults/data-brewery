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

import scala.reflect.runtime.universe._

object ReflectionHelper {
  def isAbstract[T: TypeTag]: Boolean = {
    typeTag[T].tpe.typeSymbol.asClass.isAbstract
  }

  def getCompanion[T: TypeTag]: Any = {
    val mirror = typeTag[T].mirror
    val companionModule = typeTag[T].tpe.typeSymbol.companion.asModule
    mirror.reflectModule(companionModule).instance
  }

  def createInstance[T: TypeTag]: T = {
    val tag = typeTag[T]
    val mirror = tag.mirror
    val clazz = mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
    clazz.newInstance().asInstanceOf[T]
  }

  def createInstance[T: TypeTag](args: AnyRef*): T = {
    val tag = typeTag[T]
    val mirror = tag.mirror
    val clazz = mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
    clazz.getConstructor(args.map(_.getClass):_*).newInstance(args).asInstanceOf[T]
  }

  def hasConstructor[T: TypeTag](args: AnyRef*): Boolean = {
    val tag = typeTag[T]
    val mirror = tag.mirror
    val clazz = mirror.runtimeClass(tag.tpe.typeSymbol.asClass)
    try {
      clazz.getConstructor(args.map(_.getClass): _*)
      true
    } catch {
      case _ : NoSuchMethodException => false
    }
  }
}
