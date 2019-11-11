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

package com.dataintoresults.etl.impl

import scala.collection.mutable.Publisher
import scala.xml.{XML, Elem, Node, NodeSeq}
import scala.xml.transform.{RewriteRule, RuleTransformer}
import scala.util.{Try, Success, Failure}

import java.nio.file.{Files, Path, Paths}

import org.apache.commons.lang3.SystemUtils

import play.api.libs.json.JsObject
import play.api.libs.json.Json
import play.api.Logger
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.dataintoresults.util.Using.using
import com.dataintoresults.util.XmlHelper._
import com.dataintoresults.etl.core._
import com.dataintoresults.etl.impl.source._
import com.dataintoresults.etl.impl.process._
import com.dataintoresults.etl.datastore.flat.FlatFileStore
import com.dataintoresults.etl.datastore.googleAnalytics.GoogleAnalyticsStore
import com.dataintoresults.etl.datastore.googleSheet.GoogleSheetStore
import com.dataintoresults.etl.datastore.sftp.SftpStore
import com.dataintoresults.etl.datastore.sql.SqlStore
import com.dataintoresults.etl.datastore.sql.SqlTable
import com.dataintoresults.etl.datastore.sql.PostgreSqlStore
import com.dataintoresults.etl.datastore.sql.MySqlStore
import com.dataintoresults.etl.datastore.sql.MsSqlStore
import com.dataintoresults.etl.datastore.sql.H2Store
import com.dataintoresults.etl.datastore.odoo.OdooStore
import com.dataintoresults.etl.datastore.hubspot.HubspotStore
import com.dataintoresults.etl.datastore.googleSearchConsole.GoogleSearchStore
import com.dataintoresults.etl.datastore.mongodb.MongoDbStore
import com.dataintoresults.etl.datastore.http.HttpStore
import com.dataintoresults.etl.datastore.bitmovin.BitmovinStore
import com.dataintoresults.etl.explorer._
import scala.xml.XML
import scala.collection.JavaConverters._


import java.nio.file.{Files, Paths}
import java.io.IOException
import scalaz.effect.IoExceptionOr

trait DataStoreFactory  {
  
  def apply(xmlNode: scala.xml.Node, etl: EtlImpl): DataStore 
  
  def dataStoreType: String 
  
}


object EtlImpl {
  private lazy final val defaultConfig = ConfigFactory.load()
}

class EtlImpl(private val _config : Config = EtlImpl.defaultConfig,
  val overseer : Overseer = new OverseerBasic()) extends Etl with Publisher[JsObject] {
  
  val logger: Logger = Logger(this.getClass())
  
  private val dataStoreFactories = collection.mutable.Map[String, DataStoreFactory]()
  
  /*
   * Class members 
   */
  
  // Start with an empty data warehouse
  private var dw = new DataWarehouseImpl()
  
  def dataWarehouse : DataWarehouse = dw
  
  /*
   * Etl trait implementations
   */
  
	def load(dwXml : scala.xml.Node) : Unit = {
	  dw = fromXmlDataWarehouse(dwXml)   
	}
	

	def load(path: Path): Unit = {		
		load(loadPath(path).filter(_.isInstanceOf[Elem]).head)
	}

	private def loadPath(path: Path): Seq[scala.xml.Node] = {
		// placeHolder is a dirty way to allow files with many toplevel elements
		val buffer = "<placeHolder>"+new String(Files.readAllBytes(path)) +"</placeHolder>"
		val xml = XML.loadString(buffer)

		val parentPath = path.toAbsolutePath.getParent()

    val includes = new RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case e: Elem if e.label == "include" => {
					val includePath = e \@? "path" getOrElse {
						throw new RuntimeException("There is a include element in file ${path.toString} without a mandatory path attribute.")
					}

					// Is there some pattern matching or not
					if(includePath contains "*") {
						val separator = parentPath.getFileSystem().getSeparator()
						// Unsure why, but the matcher doesn't like filesystem separator
						val matcherPattern = "glob:" + parentPath.toString.replace(separator, "/") + "/" + includePath
						val matcher = parentPath.getFileSystem().getPathMatcher(matcherPattern)
						Files.walk(parentPath) 
							.iterator.asScala // Convert to Scala
							.filter( path => matcher.matches(path)) // filter only matched
							.map( path => loadPath(parentPath.resolve(path))) // load XML
							.flatten.toSeq
					}
					else {
						if(parentPath.resolve(includePath) == null) {
							throw new RuntimeException(s"Can't find $includePath inside directory $parentPath")
						}
						loadPath(parentPath.resolve(includePath))
					}
				}
				case e: Elem if e.attribute("contentPath").isDefined => {
					val contentPath = e \@ "contentPath"
					val content = new String(Files.readAllBytes(parentPath.resolve(contentPath)))
					e.copy(child = e.child ++ scala.xml.PCData(content))
				}
        case n => n
      }
    }

		// We take xml.child to remove the placeHolder hack
		new RuleTransformer(includes).transform(xml.child)
	}
  
	def clear() : Unit = {
	  dw = new DataWarehouseImpl()
	}
	
	def save() : scala.xml.Node = {
	  toXmlDataWarehouse()
	}
	
	
	def previewTableFromDataStore(dataStore: String, table: String, nbRows: Int = 10) : DataSet = {	  
	  publish(Json.obj("step" -> s"Get data from ${dataStore}.${table} (${nbRows} rows max)"))
	  val ds = dw.datastores find { _.name == dataStore } getOrElse {throw new RuntimeException(s"No datastore like ${dataStore}.")}
		val tab = ds.tables find { _.name == table } getOrElse {throw new RuntimeException(s"No table like ${table} in datastore ${dataStore}.")}
  
		using(tab.read()) { df =>
  		
  		val sb = Seq.newBuilder[Seq[Any]]
  		
  		var i = 0;
  		while(df.hasNext() && i < nbRows) {
  		  val res = df.next()
  	    sb += res
  	    i += 1
  		}
	    new DataSetImpl(df.structure, sb.result())	  
		}
	}
	
	
	def previewTableFromModule(module: String, table: String, nbRows: Int = 10) : DataSet = {	  
	  publish(Json.obj("step" -> s"Get data from ${module}.${table} (${nbRows} rows max)"))
	  val mod = dw.modules find { _.name == module } getOrElse {throw new RuntimeException(s"No module like ${module}.")}
		val tab = mod.tables find { _.name == table } getOrElse {throw new RuntimeException(s"No table like ${table} in module ${module}.")}
  
		using(tab.read()) { df =>
  		
  		val sb = Seq.newBuilder[Seq[Any]]
  		
  		var i = 0;
  		while(df.hasNext() && i < nbRows) {
  		  val res = df.next()
  	    sb += res
  	    i += 1
  		}
	    new DataSetImpl(df.structure, sb.result())	  
		}
	}
	
	
	
	def exploreDataStore(dataStore: String) : DataStoreExplorer = {  
	  publish(Json.obj("step" -> s"Explorer datastore ${dataStore}"))
	  val ds = dw.datastores find { _.name == dataStore } getOrElse {throw new RuntimeException(s"No datastore like ${dataStore}.")}
	  
	  if(!ds.isInstanceOf[SqlStore])
	    throw new RuntimeException(s"Datastore ${dataStore} is not SQL and therefore not explorable.")
	  
	  val dsSQL = ds.asInstanceOf[SqlStore]
	  
	  val result = new StringBuilder(1000)

	  DataStoreExplorer(dataStore, dsSQL.getModulesFromDatabaseMetaData() map { schema =>
	    SchemaExplorer(schema, dsSQL.getTablesFromDatabaseMetaData(schema) map { table =>
	      TableExplorer(table)
	    }
	    )
	  })
	}
	  
	
	
	def runQuery(dataStore: String, query: String) : DataSet = {
	  publish(Json.obj("step" -> s"Get data from ${dataStore} with query ${query}"))
	  val ds = dw.datastores find { _.name == dataStore } getOrElse {throw new RuntimeException(s"No datastore like ${dataStore}.")}
	  val dsSQL = ds.asInstanceOf[SqlStore]
	  
		using(dsSQL.createDataSource(query)) { df =>
  		
  		val sb = Seq.newBuilder[Seq[Any]]
  		
  		var i = 0;
  		while(df.hasNext()) {
  		  val res = df.next()
  	    sb += res
  	    i += 1
  		}
	    new DataSetImpl(df.structure, sb.result())	 
		  
	  }
	}
	
	def addDataStore(dsXml: scala.xml.Node) : Unit = {
	  dw.addDatastore(fromXmlDataStore(dsXml))
	}
	
	def removeDataStore(dataStoreName: String) : Unit = {
	  dw.removeDatastore(dataStoreName)
	}
	
	def addTable(dataStoreName: String, tableXml: scala.xml.Node) : Unit = {
	  val ds = findDataStore(dataStoreName)
	  ds.addTable(tableXml)
	}
	
	def removeTable(dataStoreName: String, tableName: String) : Unit = {
	  val ds = findDataStore(dataStoreName)
	  ds.removeTable(tableName)
	}
	
	def close() : Unit = {
	  dw.close()
	  overseer.close()
	}
	
		
	def copy(dataStoreFrom: String, tableFrom: String, dataStoreTo: String, tableTo: String) : Unit = {
	  val dsFrom = dw.datastores find { _.name == dataStoreFrom } getOrElse {throw new RuntimeException(s"No datastore like ${dataStoreFrom}.")}
		val tabFrom = dsFrom.tables find { _.name == tableFrom } getOrElse {throw new RuntimeException(s"No table like ${tableFrom} in datastore ${dataStoreFrom}.")}

		val dsTo = dw.datastores find { _.name == dataStoreTo } getOrElse {throw new RuntimeException(s"No datastore like ${dataStoreTo}.")}
		val tabTo = dsTo.tables find { _.name == tableTo } getOrElse {throw new RuntimeException(s"No table like ${tableTo} in datastore ${dataStoreTo}.")}
		
	  publish(Json.obj("step" -> s"Copy ${dsFrom.name}.${tabFrom.name} to ${dsTo.name}.${tabTo.name}"))
	  
		overseer.runJob(tabFrom.read(), tabTo.write())
	}
	
	/**
	 * Replicate works like copy, but it will first drop the table, if possible, then create it.
	 */
	def replicate(dataStoreFrom: String, tableFrom: String, dataStoreTo: String, tableTo: String) : Unit = {
	  val dsFrom = dw.datastores find { _.name == dataStoreFrom } getOrElse {throw new RuntimeException(s"No datastore like ${dataStoreFrom}.")}
		val tabFrom = dsFrom.tables find { _.name == tableFrom } getOrElse {throw new RuntimeException(s"No table like ${tableFrom} in datastore ${dataStoreFrom}.")}

		val dsTo = dw.datastores find { _.name == dataStoreTo } getOrElse {throw new RuntimeException(s"No datastore like ${dataStoreTo}.")}
		val tabTo = dsTo.tables find { _.name == tableTo } getOrElse {throw new RuntimeException(s"No table like ${tableTo} in datastore ${dataStoreTo}.")}

	  publish(Json.obj("step" -> s"Replicate ${dsFrom.name}.${tabFrom.name} to ${dsTo.name}.${tabTo.name}"))
	  
		dropTableIfExists(tabTo);
		createTable(tabTo);
		
		overseer.runJob(tabFrom.read(), tabTo.write())
		
	}
	
	def addGoogleAnalyticsDataStore(dataStore: String, viewId: String) = {
    val spec = 
      <datastore name="ga" type="googleAnalytics" viewId={viewId}>
			</datastore>
    
    val ds = fromXmlDataStore(spec)
    
    dw.addDatastore(ds)
  }
  
  def addGoogleAnalyticsTable(dataStore: String, name: String, dimensions: Seq[String], measures: Seq[String]) = {
    val ds = findDataStore(dataStore)
    val spec = 
      <table name={name} startDate="365daysAgo" endDate="yesterday">
			  { dimensions map { d => <column name={ d.slice(3, d.length) } type="varchar" gaName={d} gaType="dimension"/>} }
				{ measures map { m => <column name={ m.slice(3, m.length) } type="bigint" gaName={m} gaType="measure"/>} }
			</table>
		ds.addTable(spec) 
  }
  
  def findDataStore(dataStore: String) : DataStore = {
    dw.datastores find { _.name == dataStore } getOrElse { throw new RuntimeException(s"Data store '${dataStore}' not found.") }
  }
  
	
	def findModule(module: String) : Module = {
    dw.modules find { _.name == module } getOrElse { throw new RuntimeException(s"Module with name '${module}' not found.") }
	}
	
	def findProcess(process: String) : Process = {
    dw.processes find { _.name == process } getOrElse { throw new RuntimeException(s"Process with name '${process}' not found.") }
	}
	
	def addSqlDataStore(name: String, backend: String, host: String, database: String, user: String, password: String) : Unit = {
	  val spec = 
      <datastore name={name} type={backend} host={host} database={database} user={user} password={password}>
			</datastore>
    
    val ds = fromXmlDataStore(spec)
    
    dw.addDatastore(ds)
	}
	
	def addSqlTable(dataStore: String, name: String, schema: String, columns: Seq[(String, String)]) : Unit = {
    val ds = findDataStore(dataStore)
    val spec = 
      <table name={name} schema={schema}>
			  { columns map { case (n, t) => <column name={n} type={t}/>} }
			</table>
		ds.addTable(spec) 
	}
	
	
	
	def runModule(moduleName: String) : Unit = {
	  publish(Json.obj("module" -> moduleName, "step" -> "init"))
	  
	  val module = findModule(moduleName).asInstanceOf[ModuleImpl]
	  
	  publish(Json.obj("module" -> moduleName, "step" -> s"Ensure existence of schema ${module.schema}"))
	  logger.info(s"Running module $moduleName: Ensure existence of schema ${module.schema}")
	  module.dataStore.asInstanceOf[SqlStore].createSchema(module.schema)
	  /*
	  module.replicateDataStore foreach { replicate =>
	    publish(Json.obj("module" -> moduleId, "step" -> s"Replicate ${replicate.dataStore.name}"))
	    runReplicate(module, replicate)  }	  
	  */
	  module.moduleTables.filter { _.hasSource } foreach { table =>
	    publish(Json.obj("module" -> moduleName, "step" -> s"Sourcing for ${table.name}"))	    
	    logger.info(s"Running module $moduleName: Sourcing for ${table.name}")
      val sqlStore = module.sqlStore
	    val source:EtlSource = table.source.asInstanceOf[EtlSource]
  	  val schema = table.schema
  	  
	    val query = source.processOnModule(this, module, table)
	    
			// If there is no query, it' means that we have nothing to do (scripting)
	    query.foreach { query =>
				table.mergeStrategy match {
					// In case of a rebuild, we scratch the current table and create a new one
					case ModuleTable.Rebuild =>
						sqlStore.dropTableIfExists(schema, table.name)
						sqlStore.createTableAs(schema, table.name, query)
						
					// In case of overwrite, we rename the current table as _old, 
					// put new data in a _new table, then create a new table merging both _old and _new
					case ModuleTable.Overwrite => 
						// Checking if the pre-requisites are validated 	      
						val businessKey = table.businessKeys       
						if(businessKey.length == 0)
							throw new RuntimeException(s"You need to have a businessKeys attribute in for module table ${module.name}.${table.name} to use the overwrite strategy")
							
						// Just in case something did go wrong last update
						// We renamed the current as old, but where unable to create a new current.
						if(sqlStore.tableExists(schema, table.name + "_old") && !sqlStore.tableExists(schema, table.name)) {
							sqlStore.renameTable(schema, table.name + "_old", table.name)
						}
													
						// If the current table exists, update it
						if(sqlStore.tableExists(schema, table.name)) {
							// Source for old data
							sqlStore.dropTableIfExists(schema, table.name + "_old")
							sqlStore.renameTable(schema, table.name, table.name + "_old")
								
							// Source for new data
							sqlStore.dropTableIfExists(schema, table.name +"_new");
							sqlStore.createTableAs(schema, table.name +"_new", query);	
							
							val columns = if(table.columns.size ==  0) {
								sqlStore.getTableFromDatabaseMetaData(schema, table.name +"_new").columns
							}
							else {
								table.columns // We need to remove structural columns (key, update_timestamp, ...) 
							}      	  
							
							// Merge      		 
							sqlStore.createTableAs(schema, table.name, sqlStore.queryMergeNewOldNoFullJoin(table.name, schema, columns, businessKey))
						}
						// If the current table doesn't exists start from scratch
						else {
							sqlStore.dropTableIfExists(schema, table.name +"_new");
							sqlStore.createTableAs(schema, table.name +"_new", query);		   
							
							val columns = if(table.columns.size ==  0) {
								sqlStore.getTableFromDatabaseMetaData(schema, table.name +"_new").columns
							}
							else {
								table.columns // We need to remove structural columns (key, update_timestamp, ...) 
							}    
													
							// Merge      			  	  
							sqlStore.createTableAs(schema, table.name, 
								s"""
								select 
									cast(row_number() over () as bigint) as ${sqlStore.columnEscapeStart}${table.name}_key${sqlStore.columnEscapeStart},  
									*,
									current_timestamp as ${sqlStore.columnEscapeStart}create_timestamp${sqlStore.columnEscapeStart},  
									current_timestamp as ${sqlStore.columnEscapeStart}update_timestamp${sqlStore.columnEscapeStart} 
								from ${sqlStore.sqlTablePath(schema, table.name + "_new")} n
								""");	
		
						}	  
						
					case ModuleTable.TrackChange =>
						throw new RuntimeException("Strategy ModuleTable.TrackChange not implemented yet.")
				}
			}
	    
	  }
	}
	
	
	def runDataStore(dataStoreName: String) : Unit = {
	  publish(Json.obj("dataStore" -> dataStoreName, "step" -> "run datastore"))
	  
	  val dataStore = findDataStore(dataStoreName)
	  
	  dataStore.tables filter (_.hasSource()) foreach { table =>
	    runDataStoreTable(dataStore, table)
	  }
	}
	
	def runDataStoreTable(dataStoreName: String, tableName: String) : Unit = {	  
	  val dataStore = findDataStore(dataStoreName)
	  
	  val table = dataStore.table(tableName) getOrElse
	    (throw new RuntimeException(s"Can't find table ${tableName} in data store ${dataStoreName} for processing."))
	    
	  
	  runDataStoreTable(dataStore, table)
	}

	

	def runShellCommand(shellCommand: String, parameters: Seq[String]) : Unit = {
		import sys.process._
		val parsedParameter = parameters.map(p => ParameterParser.parse(p, this.config))
		Try((shellCommand + parsedParameter.mkString(" ", " ", "")).!) match {
			// Adding .bat is it doesn't find the file.
			case Failure(e : IOException) if SystemUtils.IS_OS_WINDOWS => 
				(shellCommand+".bat" + parsedParameter.mkString(" ", " ", "")).!
			case Failure(e) => throw e
			case Success(code) => Unit
		}
	}
	
	def runProcess(processName: String) : Unit = {	  
		val process = findProcess(processName)
		
	  publish(Json.obj("process" -> processName, "step" -> "start"))
		
		process.tasks foreach { task =>
			task.taskType match {
				case Task.MODULE => {
					publish(Json.obj("process" -> processName, "step" -> "runModule", "module" -> task.module))
					runModule(task.module)
				}
				case Task.DATASTORE => {
					publish(Json.obj("process" -> processName, "step" -> "runDatastore", "datastore" -> task.datastore))
					runDataStore(task.datastore)
				}
				case Task.SHELL => {
					publish(Json.obj("process" -> processName, "step" -> "runShell", "shellCommand" -> task.shellCommand))
					runShellCommand(task.shellCommand, task.shellParameters)
				}
			}
		}
	  publish(Json.obj("process" -> processName, "step" -> "end"))
	}
	
	
	
/*	def runReplicate(module: Module, replicate: ReplicateDataStore) = {
	  val dsFrom = replicate.dataStore
    val dsTo = module.dataStore

	  
	  dsFrom.tables map {	tabFrom => 
	    publish(Json.obj("module" -> module.id, "step" -> s"Sourcing for m${module.id}_${module.name}.${tabFrom.name}"))
	    
	    val tabTo = module.tables find { _.name == tabFrom.name } getOrElse { throw new RuntimeException(s"Module with id ${module.id} doesn't recognize a table named ${tabFrom.name}.") }
  
  		dropTableIfExists(tabTo);
  		createTable(tabTo);
  		
		  overseer.runJob(tabFrom.read(), tabTo.write())
		  /*
  		using(tabFrom.read()) { input =>
  		  using(tabTo.write()) { output =>
  		    input foreach output.put 
  		  }
  	  }*/
	  }
  }*/
	
	
  
  def registerDataStoreFactory(dataStoreType: String, factory: DataStoreFactory): Unit = 
    dataStoreFactories(dataStoreType) = factory
	
  /*
   * Others functions
   */	
	
	def config : Config = _config
	
  private def dropTableIfExists(table: Table) : Unit = {
    table.dropTableIfExists()
  }
  
  private def createTable(table: Table) : Unit = {
    table.createTable()
  }
  
  
	private def runDataStoreTable(dataStore: DataStore, table: Table) : Unit = {
	  val dataStoreName = dataStore.name
	  val tableName = table.name
	  
	  publish(Json.obj("dataStore" -> dataStoreName, "step" -> tableName))
	  logger.info(s"Running datatstore table $dataStoreName.$tableName")
	  	  
	  // Ensure there is a source to process
	  if(!table.hasSource) 
	    throw new RuntimeException(s"There is no source to process ${dataStoreName}.${tableName}.")
	    
	  // Ensure there is a source to process
	  if(!table.canWrite) 
	    throw new RuntimeException(s"The table ${dataStoreName}.${tableName} can't be written.")
	    
	    
	  // We grab the data source from the Source
	  table.source match {
			case src: SourceQuery => {
				val dsSql = dataStore.asInstanceOf[SqlStore]
				val tableSql = table.asInstanceOf[SqlTable]
				dsSql.createTableAs(tableSql.schema, tableSql.name, src.query)
			}
			case src: EtlSource => src.processOnDataStore(this, dataStore, table) foreach { source =>
				val sink = table.write()
				overseer.runJob(source, sink)	  
			}
		}
	}

    
	private def fromXmlDataWarehouse(dwXml : scala.xml.Node) : DataWarehouseImpl = {
		dw = new DataWarehouseImpl()

		/* TODO :  
		 * there is a need to do addDatastore and not go throught a functional ways with map
		 * as a datastore can reference a previous datastore.
		 * This should change.
		 */
		(dwXml  \ "datastore").foreach( dsXml =>
			dw.addDatastore(fromXmlDataStore(dsXml))
		  )
		  
		(dwXml  \ "module").foreach( modXml =>
			dw.addModule(fromXmlModule(modXml))
		  )
		
		dw.processes = (dwXml  \ "process") map { dsXml =>	fromXmlProcess(dsXml)	}
		  
		dw
	}
		
	private def toXmlDataWarehouse() : scala.xml.Node = {
	  val dwXml = <datawarehouse>
		    { dw.datastores map { ds => ds.toXml() }	}
		    { dw.modules map { mod => mod.toXml() }	}
		    { dw.processes map { process => process.toXml() }	}
			</datawarehouse>
		dwXml
	}
	
	
	private def fromXmlDataStore(dsXml : scala.xml.Node) : DataStore = {	  
		(dsXml \ "@type").text match {
			case "flat" => FlatFileStore.fromXml(dsXml, config)
			case "postgresql" => PostgreSqlStore.fromXml(dsXml, config)
			case "mysql" => MySqlStore.fromXml(dsXml, config)
			case "mssql" => MsSqlStore.fromXml(config, this, dsXml)
			case "mongodb" => MongoDbStore.fromXml(dsXml, config)
			case "sftp" => SftpStore.fromXml(dsXml, config)
			case "googleAnalytics" => GoogleAnalyticsStore.fromXml(dsXml, config)
			case "googleSheet" => GoogleSheetStore.fromXml(dsXml, config)
			case "googleSearchConsole" => GoogleSearchStore.fromXml(dsXml, config)
			case "http" => HttpStore.fromXml(dsXml, config)
			case "odoo" => OdooStore.fromXml(dsXml, config)
			case "hubspot" => HubspotStore.fromXml(dsXml, config)
			case "h2" => H2Store.fromXml(config, this, dsXml)
			case "bitmovin" => BitmovinStore.fromXml(dsXml, config)
			// Finally maybe there is a specific data store factory registered
			case dsType: String =>  dataStoreFactories.get(dsType) match {
        case Some(factory) => factory(dsXml, this)
        case None => throw new RuntimeException(s"DataStore of type ${dsType} is noot recognized")
      }
		}
	}	

	private def toXmlDataStore(ds: DataStore) : scala.xml.Node = {
	  ds.toXml()
	}
	
	
	private def fromXmlModule(modXml : scala.xml.Node) : Module = {
	  val dsName = modXml  \@? "datastore" getOrElse 
      (throw new RuntimeException(s"When you declare a module, you need to have a datastore attribute."))
			  
	  val ds = findDataStore(dsName)
	  
	  val sqlStore = ds.asInstanceOf[SqlStore]
	  
	  val mod = new ModuleImpl(this, sqlStore)
	  mod.fromXml(modXml)
	  mod
	}
	
	private def fromXmlProcess(dsXml : scala.xml.Node) : Process = {	  
		EtlProcess.fromXml(dsXml, config)
	}	
		
	
}