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

package com.dataintoresults.etl.impl

import scala.xml.XML
import scala.util.{Try}
import scala.math.BigDecimal

import java.io.StringReader
import java.time.{LocalDate, LocalDateTime}
import java.nio.file.{Files, Paths}
import java.nio.charset.Charset

import javax.mail.internet.MimeMultipart

import play.api.libs.json.Json

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import org.apache.commons.io.FileUtils

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite
import org.scalatest.Assertions._
import org.scalatest.AppendedClues._

import com.icegreen.greenmail.user.UserException;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetupTest;

import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.util.Using._
import com.dataintoresults.etl.core.ProcessResult
import com.dataintoresults.util.TwirlHelper
import org.scalatest.Ignore



class ProcessMailsTest extends FunSuite with BeforeAndAfter {
  private var greenMail : GreenMail = _

  private final val mailerUser = "mailer"
  private final val mailerPassword = "s3cr3t"

  private var mailerConfig = ConfigFactory.parseString(s"""
  dw.mailer {
    host = "localhost" // (mandatory)
    port = ${ServerSetupTest.SMTP.getPort} // (defaults to 25)
    ssl = no // (defaults to no)
    tls = yes // (defaults to no)
    tlsRequired = no // (defaults to no)
    user = ${mailerUser} // (optional)
    password = ${mailerPassword} // (optional)
    debug = no // (defaults to no, to take effect you also need to set the log level to "DEBUG" for the application logger)
    timeout = null // (defaults to 60s in milliseconds)
    connectiontimeout = null // (defaults to 60s in milliseconds)
    mock = no // (defaults to no, will only log all the email properties instead of sending an email)
    props = {}
  }
  """)

  before {
    greenMail = new GreenMail(ServerSetupTest.SMTP)
    greenMail.setUser(mailerUser, mailerPassword)
    greenMail.start()
  }

  after {
    greenMail.stop()
  }

  val dw1 = 
    <datawarehouse>
      <datastore name="dw" type="h2"/>

      <!-- Module that should work -->
      <module name="ok2" datastore="dw">
        <table name="ok" strategy="rebuild">
          <source type="query">
            select 1 as a
          </source>
        </table>
      </module>

      <!-- Module that should FAIL -->
      <module name="nok2" datastore="dw">
        <table name="nok">
          <source type="query">
            select 1/0
          </source>
        </table>
      </module>

      <process name="shouldSucceed" email="shouldSucceed@test.com">
        <task module="ok2"/>
      </process>

      <process name="shouldFail" email="shouldFail@test.com">
        <task module="nok2"/>
      </process>

      <process name="shouldWarn" email="shouldWarn@test.com">
        <task module="nok2" onError="warning"/>
        <task module="ok2"/>
      </process>

      <process name="doubleWarn" email="doubleWarn@test.com">
        <task module="nok2" onError="warning"/>
        <task module="nok2" onError="warning"/>
        <task module="ok2"/>
      </process>
    </datawarehouse>

    
  test("Check email for sucessful process ") {
    using(new EtlImpl(mailerConfig)) { implicit etl =>
      // Should not thow
      Try(etl.load(dw1)).recover { case e : Exception =>
        fail("Shouldn't throw an exception : " + e.getMessage)
      }

      Try(etl.runProcess("shouldSucceed")).recover { case e : Exception =>
        fail("Shouldn't throw an exception : " + e.getMessage)
      }

      val msg = greenMail.getReceivedMessages()(0)
      assertResult("shouldSucceed@test.com")(msg.getAllRecipients()(0).toString) withClue "Recipient email is invalid"
      assertResult("Data Brewery - shouldSucceed - Success")(msg.getSubject()) withClue "Subject email is invalid"

      msg.getContent() match {
        case mmp: MimeMultipart => 
          assert("""Process shouldSucceed ended with status Success in .+\.""".r.findFirstIn(mmp.getBodyPart(0).getContent.toString()).isDefined)
            .withClue(s""""${mmp.getBodyPart(0).getContent.toString}" should be like Process shouldSucceed ended with status Success in .+\\.""")
        case _ => fail()
      }
    }
  }

  test("Check email for doubleWarn process ") {
    using(new EtlImpl(mailerConfig)) { implicit etl =>
      // Should not thow
      Try(etl.load(dw1)).recover { case e : Exception =>
        fail("Shouldn't throw an exception : " + e.getMessage)
      }

      Try(etl.runProcess("doubleWarn")).recover { case e : Exception =>
        fail("Shouldn't throw an exception : " + e.getMessage)
      }

      val expectedBodyPattern = """Process doubleWarn ended with status Warning in .+""".r

      val msg = greenMail.getReceivedMessages()(0)
      assertResult("doubleWarn@test.com")(msg.getAllRecipients()(0).toString) withClue "Recipient email is invalid"
      assertResult("Data Brewery - doubleWarn - Warning")(msg.getSubject()) withClue "Subject email is invalid"
 
      msg.getContent() match {
        case mmp: MimeMultipart => 
          val body = TwirlHelper.cleanTxt(mmp.getBodyPart(0).getContent.toString)
          // TwirlHelper is used because under windows there is a \r\n instead of \n
          assert(expectedBodyPattern.findFirstIn(body).isDefined)
            .withClue(s""""${mmp.getBodyPart(0).getContent.toString}" is not what was expected.""")
            assertResult(8)(body.split("\n").length)
              .withClue("Body should be 8 lines long")
        case _ => fail()
      }
    }
  }


}