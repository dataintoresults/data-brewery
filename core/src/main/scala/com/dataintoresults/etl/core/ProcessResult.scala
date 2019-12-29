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

import java.time.LocalDateTime
import java.time.Duration
import com.dataintoresults.util.TimeHelper

object ProcessResult {  
  sealed trait ProcessStatus { 
    def criticity: Int /* Fpr orderng */
  }
  object Unprocessed extends ProcessStatus { 
    override def toString(): String = "Unprocessed"
    def criticity: Int = -5
  }
  object Success extends ProcessStatus { 
    override def toString(): String = "Success"
    def criticity: Int = 0
  }
  object Warning extends ProcessStatus { 
    override def toString(): String = "Warning"
    def criticity: Int = 5
  }
  object Error extends ProcessStatus { 
    override def toString(): String = "Error"
    def criticity: Int = 10
  }
}

case class ProcessResult(status: ProcessResult.ProcessStatus,
  processed: Any,
  message: String,
  startDate: LocalDateTime = LocalDateTime.now(), endDate: LocalDateTime = LocalDateTime.now(),
  childs: Seq[ProcessResult] = Seq.empty) {

  def listFirstLevelTasks(): Seq[ProcessResult] = {
    childs.flatMap { child =>
      if(child.isTask)
        Seq(child)
      else 
        child.listFirstLevelTasks()
    }
  }

  def isProcess = processed.isInstanceOf[Process]
  def isTask = processed.isInstanceOf[Task]

  def task = processed.asInstanceOf[Task]

  def warnings = childs.filter(_.status == ProcessResult.Warning)

  def errors = childs.filter(_.status == ProcessResult.Error)

  def unprocessedChilds = childs.filter(_.status == ProcessResult.Unprocessed)
  def successChilds = childs.filter(_.status == ProcessResult.Success)
  
  def duration = Duration.between(startDate, endDate)
}