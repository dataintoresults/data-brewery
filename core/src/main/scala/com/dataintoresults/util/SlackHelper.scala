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

package com.dataintoresults.util

import scala.concurrent._
import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import play.api.libs.json.JsValue
import play.api.libs.ws.StandaloneWSClient
import play.api.libs.mailer._
import play.api.libs.json.JsValue
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import Using._
import scala.util.{Try, Success, Failure}

object SlackHelper {
  def post(webhook: String,
      payload: JsValue
    ): Unit = {
    import play.api.libs.ws.JsonBodyReadables._
    import play.api.libs.ws.JsonBodyWritables._
    
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    // Try to set the system.terminate()
    val res = Try {
      using(StandaloneAhcWSClient()) { ws =>
        val resp = ws.url(webhook)
          .withRequestTimeout(10000.millis)
          .addHttpHeaders(("Content-type", "application/json"))
          .post(payload)

        Try(Await.result(resp, 10 second)).fold(
          ex => throw new RuntimeException(s"Slack webhook message post didn't work. Exception ${ex.getClass}: ${ex.getMessage()}"),
          resp => if(resp.status != 200)
            throw new RuntimeException(s"Slack webhook message post didn't work. Return code ${resp.status} with body : ${resp.body}")
        )
      }
    }
    
    system.terminate()

    // rethrow the exception if needed
    res.recover {
      case ex => throw ex
    }
  }
}
