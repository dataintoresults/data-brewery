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

package com.dataintoresults.etl.impl.process


import scala.xml.{Attribute, Elem, Node, Null}

import com.typesafe.config.Config

import play.api.libs.json.Json
import play.api.Logger

import com.dataintoresults.util.XmlHelper._

import com.dataintoresults.etl.core._
import com.dataintoresults.etl.core.EtlParameterHelper._
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.etl.impl._
import com.dataintoresults.util.MailHelper
import com.dataintoresults.util.TwirlHelper
import scala.util.Try
import com.dataintoresults.util.TimeHelper
import java.time.LocalDateTime
import com.dataintoresults.util.SlackHelper
import scala.util.Success


object ProcessRunner {
  val logger: Logger = Logger(this.getClass())


  def processTaskResult(task: Task, result: Try[Unit]): (ProcessResult.ProcessStatus, String) = {
    result.fold(
      ex => task.onError match {
        case Task.OnErrorError =>  {
          logger.error(task.name + " : " + ex.getMessage())
          (ProcessResult.Error, ex.getMessage())
        }
        case Task.OnErrorWarning => {
          logger.warn(task.name + " : " + ex.getMessage())
          (ProcessResult.Warning, ex.getMessage())
        }
        case Task.OnErrorSuccess => {
          logger.info(task.name + " : " + ex.getMessage())
          (ProcessResult.Success, ex.getMessage())
        }
      },
      v => {
        (ProcessResult.Success, s"Task ${task.name} is a success")
      }
    )
  }

  def run(etl: EtlImpl, processName: String): ProcessResult = {
    val process = etl.findProcess(processName)
    
    etl.publish(Json.obj("process" -> processName, "step" -> "start"))
    
    var shouldStop = false

    val taskResults = process.tasks.map{ task => 
      if(shouldStop) {
        ProcessResult(ProcessResult.Unprocessed, task, s"Task $task.name unprocessed", LocalDateTime.now(), LocalDateTime.now(), Seq.empty)
      }
      task.taskType match {
        // If we don't process
        case _ if shouldStop => {
          ProcessResult(ProcessResult.Unprocessed, task, s"Task ${task.name} is skipped due to upstream errors", 
            LocalDateTime.now(), LocalDateTime.now(), Seq.empty)
        }
        case Task.MODULE => {
          val startDate = LocalDateTime.now()
          etl.publish(Json.obj("process" -> processName, "step" -> "runModule", "module" -> task.module))
          val (status, message) = processTaskResult(task, Try(etl.runModule(task.module)))
          if(status == ProcessResult.Error)
            shouldStop = true
          ProcessResult(status, task, message, startDate, LocalDateTime.now(), Seq.empty)
        }
        case Task.DATASTORE => {
          val startDate = LocalDateTime.now()
          etl.publish(Json.obj("process" -> processName, "step" -> "runDatastore", "datastore" -> task.datastore))
          val (status, message) = processTaskResult(task, Try(etl.runDataStore(task.datastore)))
          if(status == ProcessResult.Error)
            shouldStop = true
          ProcessResult(status, task, message, startDate, LocalDateTime.now(), Seq.empty)
        }
        case Task.SHELL => {
          val startDate = LocalDateTime.now()
          etl.publish(Json.obj("process" -> processName, "step" -> "runShell", "shellCommand" -> task.shellCommand))
          val (status, message) = processTaskResult(task, Try(etl.runShellCommand(task.shellCommand, task.shellParameters)))
          if(status == ProcessResult.Error)
            shouldStop = true
          ProcessResult(status, task, message, startDate, LocalDateTime.now(), Seq.empty)
        }
      }
    }

    val result = aggregateFromTasks(process, taskResults)

    if(process.emails.nonEmpty && process.emailWhen.contains(result.status)) {
  
      val subject = s"Data Brewery - ${process.name} - ${result.status}"

      val bodyText: String = TwirlHelper.cleanTxt(com.dataintoresults.etl.mail.ProcessResult.txt.mail(process, result).body)

      val bodyHtml: String = com.dataintoresults.etl.mail.ProcessResult.html.mail(process, result).body

      MailHelper.sendEmail(etl.config.getConfig("dw.mailer"), process.emails, subject, bodyText = Some(bodyText), bodyHtml = Some(bodyHtml))

      
    }

    if(process.slackWebhook.isDefined && process.slackWhen.contains(result.status)) {      
      val postText: String = TwirlHelper.cleanTxt(com.dataintoresults.etl.mail.ProcessResult.txt.slack(process, result).body)

      SlackHelper.post(process.slackWebhook.get, Json.parse(postText))
    }

    result.status match {
      case ProcessResult.Error => logger.error(s"${processName} : Ended with an error")
      case ProcessResult.Warning => logger.warn(s"${processName} : Ended with a wanring")
      case ProcessResult.Success => logger.info(s"${processName} : Ended successfuly")
      case ProcessResult.Unprocessed => throw new RuntimeException("Internal error : A process ended with status Unprocessed. Please contact support.")
    }
    etl.publish(Json.obj("process" -> processName, "step" -> "end"))
    
    result
  }


  private def aggregateFromTasks(process: Process, taskResults: Seq[ProcessResult]): ProcessResult = {
    if(taskResults.isEmpty) {
      ProcessResult(ProcessResult.Success, process, "No task to process", LocalDateTime.now(), LocalDateTime.now(), Seq.empty)
    }
    else {
      val startDate = taskResults.map(_.startDate).min(TimeHelper.localDateTimeOrdering)
      val endDate = taskResults.map(_.endDate).max(TimeHelper.localDateTimeOrdering)

      val status = taskResults.map(_.status).maxBy(_.criticity)

      val warningCount = taskResults.count(_.status == ProcessResult.Warning)
      val errorCount = taskResults.count(_.status == ProcessResult.Error)

      val message = s"Process ended"

      ProcessResult(status, process, message, startDate, endDate, taskResults)
   }
  }
}