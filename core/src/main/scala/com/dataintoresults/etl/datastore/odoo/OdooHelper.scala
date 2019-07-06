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

package com.dataintoresults.etl.datastore.odoo

import java.net.URL
import java.util.Collections._
import java.util.Arrays._
import java.util.Map
import java.util.HashMap

import scala.collection.JavaConversions._

import org.apache.xmlrpc.client.XmlRpcClient
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl
import org.apache.xmlrpc.client.XmlRpcSunHttpTransportFactory

import play.api.libs.json._
import play.api.libs.functional.syntax._

/**
 * Utility functions for Odoo management.
 */
object OdooHelper {
  /*
   * Return the server version, first cell major, version, second cell minor version
   * Return [0,0] if something goes wrong beside an exception.
   */
  def version(url: String): Array[String] = {    
    val client = new XmlRpcClient();
    
    val common_config = new XmlRpcClientConfigImpl();
    common_config.setServerURL(new URL(s"${url}/xmlrpc/2/common"))
    
    val record = client.execute(common_config, "version", emptyList()).asInstanceOf[Map[String, Object]]
    
    val version = record.get("server_version_info")
    
    if(version == null)
      Array("0","0")
    else 
      version.asInstanceOf[Array[Object]].map(i => i.toString)
  }
    
  /*
   * Get the uid (unique identifier) of a username (login access)
   */
  def authentificate(url: String, db: String, username: String, password: String): Int = {  
    val client = new XmlRpcClient();
    
    val common_config = new XmlRpcClientConfigImpl();
    common_config.setServerURL(new URL(s"${url}/xmlrpc/2/common"))   
    
    val uid : Int = client.execute(
        common_config, "authenticate", 
          asList(db, username, password, emptyMap()))
        .asInstanceOf[Int]    
    
    return uid
  }
  
  /*
   * Check if the uid can read the model
   */
  def checkReadAccess(url: String, db: String, uid: Int, password: String, resource: String): Boolean = {
    val models = new XmlRpcClient() {{
    setConfig(new XmlRpcClientConfigImpl() {{
        setServerURL(new URL(String.format("%s/xmlrpc/2/object", url)));
      }});
    }};
    models.execute("execute_kw", asList(
        db, uid, password,
        resource, "check_access_rights",
        asList("read"),
        new HashMap()
    )).asInstanceOf[Boolean]
  }
  
  /*
   * Return the list of records in the model that match the filter.
   * If you don't need a filter, don't give a parameter or give null
   * If you provide a columns parameter, only those column will be returned (this one is null by default)
   */
  def read(url: String, db: String, uid: Int, password: String, model: String, columns: Seq[String] = null, filter: Array[Array[Any]] = null): Array[JsObject] = {
    val models = new XmlRpcClient() {{
    setConfig(new XmlRpcClientConfigImpl() {{
        setServerURL(new URL(String.format("%s/xmlrpc/2/object", url)));
      }});
    }};
    
    val filters2 = 
      if(filter != null) 
        seqAsJavaList(
            filter map (arr => seqAsJavaList(arr))
        )
      else 
        asList(asList())
        
    val params = mapAsJavaMap(
      if(columns != null)
        scala.collection.Map("fields" -> seqAsJavaList(columns))
      else
        scala.collection.Map()
      )
    
    val records =
      models.execute(
        "execute_kw", asList(
            db, uid, password,
            model, "search_read",
            filters2,
            params            
          )
        ).asInstanceOf[Array[Object]]
    
    records map (r => recordToJson(r))
  }
  
  
  /*
   * Return the metadata of a Odoo model
   */
  def metadata(url: String, db: String, uid: Int, password: String, model: String): Array[JsObject] = {
    val models = new XmlRpcClient() {{
    setConfig(new XmlRpcClientConfigImpl() {{
        setServerURL(new URL(String.format("%s/xmlrpc/2/object", url)));
      }});
    }};
        
    val records =
      models.execute(
        "execute_kw", asList(
            db, uid, password,
            model, "fields_get",
            emptyList(),
            mapAsJavaMap(
              scala.collection.Map("attributes" -> asList("string", "help", "type"))
            )            
          )
        ).asInstanceOf[java.util.Map[String, Object]]
    
    (mapAsScalaMap(records).map( c => c match {
       case (field, r) => recordToJson(r).+("name", JsString(field))
    })).toArray
  }
  
  
  
  def recordToJson(record: Object): JsObject = {  
    JsObject(record.asInstanceOf[java.util.Map[String, Object]] map ( x => toJson(x._1, x._2) ))
  }
    
  private def toJson(key: String, value: Any): (String, JsValue) = {
    (key, toJson(value))
  }
  
  private def toJson(value: Any): JsValue = { 
    value match {
      case i : Integer  => Json.toJson(i.intValue())
      case s : java.lang.String  => Json.toJson(s)
      case b : java.lang.Boolean  => Json.toJson(b.booleanValue())
      case d : java.lang.Double  => Json.toJson(d.doubleValue())
      case arr : Array[Object] => Json.toJson(arr map (x => toJson(x)))
      case o : Object => recordToJson(o) 
    }
  } 
}