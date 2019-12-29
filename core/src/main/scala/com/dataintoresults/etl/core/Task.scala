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

package com.dataintoresults.etl.core

/**
 * A task is a step of a process.
 */
trait Task {
	/**
	 * Returns the name of the task.
	 */
	def name : String

	/**
	 * Returns the type of the task.
	 */
	def taskType : Task.TaskType

	/**
	 * Name of the module to execute. Throw exception if taskType != MODULE.
	 */
	def module : String

	/**
	 * Name of the module to execute. Throw exception if taskType != DATASTORE.
	 */
	def datastore : String

	/**
	 * Shell program to execute. Throw exception if taskType != SHELL.
	 */
	def shellCommand : String
	
	/**
	 * Shell parameters list. Throw exception if taskType != SHELL.
	 */
	def shellParameters: Seq[String] 

	/*
	 * Expected behavior if the task fail.
	 */
	def onError: Task.OnError
	
	/**
	 * Export the task in XML format
	 */
  def toXml() : scala.xml.Node	
}


object Task {  
	/**
	 * Allowed types of tasks.
	 */
	sealed trait TaskType

	/**
	 * Task used to run a module, use the module method to get the module name.
	 */
	case object MODULE extends TaskType

	/**
	 * Task used to run a datastore, use the module method to get the module name.
	 */
	case object DATASTORE extends TaskType

	/**
	 * Task used to run a shell program
	 */
	case object SHELL extends TaskType

	
	/**
	 * Allowed behavior on error
	 */
	sealed trait OnError
	object OnErrorError extends OnError
	object OnErrorWarning extends OnError
	object OnErrorSuccess extends OnError
}