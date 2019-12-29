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

object MailHelper {
  def sendEmail(conf: Config, 
    to : Seq[String],
    subject: String,
    bodyText : Option[String] = None,
    bodyHtml : Option[String] = None): Unit = {
    val smtpConf = SMTPConfiguration(conf)

    val mailer = new SMTPMailer(smtpConf)

    mailer.send(Email(subject, "support@databrewery.co", to, 
      bodyText = bodyText, bodyHtml = bodyHtml))

    //play.api.libs.mailer.MailerClient
  }
}
