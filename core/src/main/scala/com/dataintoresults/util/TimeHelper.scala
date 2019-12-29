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

import com.typesafe.config.Config

import play.api.libs.mailer._
import java.time.Duration
import java.time.temporal.ChronoUnit

import com.dataintoresults.util.InHelper._
import java.time.LocalDateTime

object TimeHelper {
  implicit val localDateTimeOrdering: Ordering[LocalDateTime] = _ compareTo _

  def format(duration: Duration, cut: ChronoUnit = ChronoUnit.SECONDS, maxUnit: Int = 3): String = {
    var unitUsed = 0
    (duration.toDays() match {
      case 0 if unitUsed == 0 && cut == ChronoUnit.DAYS => "0 day"
      case 0 => ""
      case 1 => unitUsed += 1; "1 day" + (if(unitUsed!=maxUnit && cut != ChronoUnit.DAYS) " " else "")
      case n => unitUsed += 1; n + " days" + (if(unitUsed!=maxUnit && cut != ChronoUnit.DAYS) " " else "")
    }) + (duration.toHours() % 24 match {
      case _ if maxUnit == unitUsed || cut == ChronoUnit.DAYS => "" 
      case 0 if unitUsed == 0 && cut == ChronoUnit.HOURS =>  "0 hour"
      case 0 if unitUsed == 0 => ""
      case 1 => unitUsed += 1; "1 hour" + (if(unitUsed!=maxUnit && cut != ChronoUnit.HOURS) " " else "")
      case n => unitUsed += 1; n + " hours" + (if(unitUsed!=maxUnit && cut != ChronoUnit.HOURS) " " else "")
    }) + (duration.toMinutes() % 60 match {
      case _ if maxUnit == unitUsed || (cut in (ChronoUnit.DAYS, ChronoUnit.HOURS)) => "" 
      case 0 if unitUsed == 0 && cut == ChronoUnit.MINUTES =>  "0 minute"
      case 0 if unitUsed == 0  => ""
      case 1 => unitUsed += 1; "1 minute" + (if(unitUsed!=maxUnit && cut != ChronoUnit.MINUTES) " " else "")
      case n => unitUsed += 1; n + " minutes" + (if(unitUsed!=maxUnit && cut != ChronoUnit.MINUTES) " " else "")
    }) + (duration.getSeconds() % 60 match {
      case _ if maxUnit == unitUsed || (cut in (ChronoUnit.DAYS, ChronoUnit.HOURS, ChronoUnit.MINUTES)) => "" 
      case 0 if unitUsed == 0 && cut == ChronoUnit.SECONDS =>  "0 second"
      case 0 if unitUsed == 0  => ""
      case 1 => unitUsed += 1; "1 second"
      case n => unitUsed += 1; n + " seconds"
    }) + (duration.toMillis() % 1000 match {
      case _ if maxUnit == unitUsed || (cut in (ChronoUnit.DAYS, ChronoUnit.HOURS, ChronoUnit.MINUTES, ChronoUnit.SECONDS)) => "" 
      case 0 if unitUsed == 0  => ""
      case 1 => unitUsed += 1; "1 millisecond"
      case n => unitUsed += 1; n + " milliseconds"
    })
  }

}
