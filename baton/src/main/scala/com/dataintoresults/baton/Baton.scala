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

package com.dataintoresults.baton

import java.io._
import java.util.UUID.randomUUID
import java.util.Properties


import scala.concurrent.duration._
import scala.xml.XML
import scala.collection.mutable.Subscriber
import scala.collection.mutable.Publisher

import org.joda.time.format.PeriodFormat
import org.joda.time.format.PeriodFormatter
import org.joda.time.format.PeriodFormatterBuilder
import org.joda.time.Period

import play.api.libs.json.JsObject

import play.api.Logger
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level

import akka.stream.ActorMaterializer
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext.Implicits._

import com.typesafe.config.ConfigFactory


import play.api.libs.ws.ahc.StandaloneAhcWSClient

import com.dataintoresults.etl.core.Etl
import com.dataintoresults.etl.impl.EtlImpl
import com.dataintoresults.etl.util.EtlHelper
import com.dataintoresults.util.Using.using

case class CliConfig(
  command: Option[String] = None,
  
  
  dw: File = new File("dw.xml"), 
  conf: Seq[File] = Seq(new File("dw.conf")),
  logLevel: Int = 0,
  
  // For read 
  readPath: Option[String] = None,
  nbRows: Option[Int] = Some(10),
  
  
  // For run-module
  module: Seq[String] = Nil,
  
  // For run-datastore
  datastore: Option[String] = None,
)
  
  
object Baton {

  LoggerFactory.getLogger("root").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.OFF)
  LoggerFactory.getLogger("com.dataintoresults.baton").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.INFO)

  private val logger: Logger = Logger(this.getClass())
  val programName = "baton"
  val programVersion = "1.0.0-M1"
  val programDate = "20190706"
  
  var nbDs: Int = _
  var nbMod: Int = _
  var distDs: String = _
  
  var globalConfig = ConfigFactory.load()
  
  def openEtl(cliConfig: CliConfig): EtlImpl = {
    val etl = new EtlImpl(globalConfig)
    
    logger.info(s"Loading datawarehouse file ${cliConfig.dw} ...")
    etl.load(XML.loadFile(cliConfig.dw))
        
    val dw = etl.dataWarehouse
    
    nbDs = dw.datastores.length
    nbMod = dw.modules.length
    
    distDs = dw.datastores.map(_.getClass.getSimpleName.replaceAll("Store","")).toSet.mkString(",")
    
    logger.info(s"Loaded ${nbDs} datastores (${distDs}) and ${nbMod} modules")
    
    
    etl      
  }
   
  def setConfig(cliConfig: CliConfig): Unit = {
    cliConfig.conf.foreach { cfgFile => 
      logger.info("Adding configuration file "+ cfgFile.toString())
        
      // Setting the file as a system property file
      val overrideConfig = ConfigFactory.parseFile(cfgFile)
      globalConfig = overrideConfig.withFallback(globalConfig)
    }
  }
   
  def process(cliConfig: CliConfig)(f: EtlImpl => Unit) = {     
     // Set logging verbosity
     if(cliConfig.logLevel < 0) {
       LoggerFactory.getLogger("root").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.OFF)
       LoggerFactory.getLogger("play").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.OFF)
       LoggerFactory.getLogger("com.dataintoresults").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.OFF)
       LoggerFactory.getLogger("com.dataintoresults.baton").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.OFF)
     }
     else if(cliConfig.logLevel > 0) {
       LoggerFactory.getLogger("play").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.INFO)
       LoggerFactory.getLogger("com.dataintoresults").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.INFO)
       LoggerFactory.getLogger("com.dataintoresults.baton").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.DEBUG)
     }
     
     setConfig(cliConfig)
          
     // Log start of the process
     val uuid = randomUUID.toString()
     val command = this.getClass.getSimpleName.replaceAll("\\$", "")
       
     using(openEtl(cliConfig)) { etl =>
       logAction(command, uuid, "START")
       
       f(etl)
     }
     
     // Log completion of the process
     logAction(command, uuid, "END")
     
  }
   
  def logAction(command: String, uuid: String, stage: String): Unit = {
    val watcherUrl = globalConfig.getString("baton.watcherUrl")
     
    if(watcherUrl == null || watcherUrl.equals("none"))
      return

     implicit val system = ActorSystem()
     implicit val materializer = ActorMaterializer()
     val ws = StandaloneAhcWSClient()
     val postedData = s"command=$command&uuid=$uuid&stage=$stage&version=${Baton.programVersion+"-"+Baton.programDate}&" + 
       s"nbds=${nbDs}&nbmod=${nbMod}&dstypes=${distDs}"
     
     if(globalConfig.getBoolean("baton.showPostedData"))
       logger.info(s"Sending $postedData to $watcherUrl")
     else 
       logger.debug(s"Sending $postedData to $watcherUrl")
     
     val response = ws.url(watcherUrl)
       .withHttpHeaders("Content-type" -> "application/x-www-form-urlencoded")
       .post(postedData).map { response =>
       logger.debug("Response from server : " + response.body)
       ws.close()
       system.terminate()
     }
   }
  
   
  def read(cliConfig: CliConfig): Unit = {
    process(cliConfig) { etl =>       
      val pathArray = cliConfig.readPath.get.split("\\.")
      
      if(pathArray.length == 2) {
        val datastore = pathArray(0)
        
        val table = pathArray(1)
        try {
          logger.info(s"Read table $table from datastore $datastore ...")
          System.out.println(EtlHelper.printDataset(etl.previewTableFromDataStore(datastore, table, cliConfig.nbRows.getOrElse(999999999))))
        }
        catch { case e: Throwable =>
          logger.info(s"Didn't work (exception ${e.getClass} : ${e.getLocalizedMessage}), try read table $table from module $datastore ...")
          System.out.println(EtlHelper.printDataset(etl.previewTableFromModule(datastore, table, cliConfig.nbRows.getOrElse(999999999))))
        }
      }
      else {        
        val module = pathArray(1)
        
        val table = pathArray(2)

        logger.info(s"Read table $table from module $module ...")
        
        System.out.println(EtlHelper.printDataset(etl.previewTableFromModule(module, table, cliConfig.nbRows.getOrElse(999999999))))
      }
    }
  }
  
  def runModule(cliConfig: CliConfig): Unit = {
    process(cliConfig) { etl =>  
      etl.subscribe(new Subscriber[JsObject, Publisher[JsObject]] {
        override def notify(pub: Publisher[JsObject], event: JsObject) = {
          val module = (event \ "module").asOpt[String]
          val step = (event \ "step").asOpt[String]
          if(module.isDefined && step.isDefined)
            logger.info(module.get + " : " + step.get)
        }
      })
      
      cliConfig.module.foreach { module =>         
        val startTime = System.currentTimeMillis()
        logger.info("Processing module " + module + " ...")
        etl.runModule(module)
        logger.info("Processing module " + module + " took " + 
            PeriodFormat.getDefault().print(new Period(System.currentTimeMillis() - startTime) ))
      }
    }
  }
  
  def runDatastore(cliConfig: CliConfig): Unit = {
    process(cliConfig) { etl =>  
      etl.runDataStore(cliConfig.datastore.get)
    }
  }
  
  val parser = new scopt.OptionParser[CliConfig]("baton") {
    head(programName, programVersion, programDate)

    opt[File]('i', "dw").valueName("<dwFile>").
      action( (x, c) => c.copy(dw = x) ).
      text("Datawarehouse XML definition file to use (default dw.xml)")
    
    opt[File]('c', "conf").unbounded().valueName("<confFile1>,<confFile2>").
      action( (x, c) => c.copy(conf = c.conf :+ x) ).
      text("Configuration files to use (always read dw.conf first).")
      
    opt[Unit]('s', "silent").action( (_, c) => c.copy(logLevel = -1) ).
      text("Disable logging messages.")
      
    opt[Unit]('v', "verbose").action( (_, c) => c.copy(logLevel = 1) ).
      text("Display more logging messages.")

    help("help").text("prints this usage text")
  
    note("")
  
    cmd("read").action( (_, c) => c.copy(command = Some("read")) ).
      text("read the content of a table.").
      children(
        arg[String]("<table>").required().action( (x, c) =>
          c.copy(readPath = Some(x)) ).text("the table to read"),          
        opt[Int]("nb-rows").action( (x, c) =>
          c.copy(nbRows = Some(x)) ).text("Number of rows to read (10 if not set)."), 
        opt[Unit]("no-limit").action( (x, c) =>
          c.copy(nbRows = None) ).text("Read an unlimited number of rows.")
      )
    
    note("")
    
    cmd("run-module").action( (_, c) => c.copy(command = Some("run-module")) ).
      text("run-module process the specified module.").
      children(
        arg[String]("<module>").required().unbounded().action( (x, c) =>
          c.copy(module = c.module :+ x ) ).text("module(s) to be processed (required)")
      )
      
    note("")
    
    cmd("run-datastore").action( (_, c) => c.copy(command = Some("run-datastore")) ).
      text("run-datastore process the specified datastore.").
      children(
        arg[String]("<datastore>").required().action( (x, c) =>
          c.copy(datastore = Some(x)) ).text("the datastore to be processed (required)")
      )
      
    
  }

  
	def main(args: Array[String]) {
    // parser.parse returns Option[C]
    parser.parse(args, CliConfig()) match {
      case Some(cliConfig) =>
        cliConfig.command match {
          case None => parser.showUsage()
          case Some("read") => read(cliConfig)
          case Some("run-module") => runModule(cliConfig)
          case Some("run-datastore") => runDatastore(cliConfig)
        }
        
      case None =>
    }
  }
}