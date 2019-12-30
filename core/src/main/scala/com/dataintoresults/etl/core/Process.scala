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
import com.dataintoresults.etl.core.Process.Structure

object Process {
	sealed trait Structure
	
	object Structure {
		object Auto extends Structure
		object Serial extends Structure
		object Dag extends Structure
	}

}

/**
 * A process is a list of tasks.
 */
trait Process {

	/**
	 * Returns the name of this process
	 */
	def name : String

	/**
	 * Returns a list of tasks
	 */
	def tasks : Seq[Task]

	/**
	 * Returns a list of email for notifications
	 */
	def emails: Seq[String] = Seq.empty
	
	/**
	 * Returns a list of case that trigger email notifications
	 */
	def emailWhen: Seq[ProcessResult.ProcessStatus] = Seq.empty
	
	/**
	 * Returns a list of case that trigger email notifications
	 */
	def slackWebhook: Option[String] = None

	/**
	 * Returns a list of case that trigger email notifications
	 */
	def slackWhen: Seq[ProcessResult.ProcessStatus] = Seq.empty
	
	/**
	 * Structure of the process:
	 *  - auto, let the structure be infered from the tasks
	 *  - serial, each task depends on the preceeding
	 *  - dag, directed acyclic graph where task predecessor 
	 *    are given with the dependOn attribute
	 */
  def structure: Seq[Structure] = Seq.empty

  def toXml() : scala.xml.Node
}