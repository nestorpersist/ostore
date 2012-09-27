/*
 *  Copyright 2012 Persist Software
 *  
 *   http://www.persist.com
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
*/

package com.persist

import scala.io.Source
import JsonOps._
import java.net.URL
import org.apache.http.conn.scheme.SchemeRegistry
import org.apache.http.params.BasicHttpParams
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager
import org.apache.http.impl.client.DefaultHttpClient
import scala.collection.JavaConversions._
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.client.methods.HttpDelete
import org.apache.http.params.HttpProtocolParams
import org.apache.http.HttpVersion
import org.apache.http.conn.scheme.Scheme
import org.apache.http.conn.scheme.PlainSocketFactory
import org.apache.http.params.HttpConnectionParams
import org.apache.http.impl.conn.SingleClientConnManager
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpGet
import org.apache.http.HttpEntity
import java.io.InputStreamReader
import java.io.BufferedReader
import org.apache.http.HttpResponse
import Exceptions._
import com.vaadin.ui.Window

class WebClient(host: String, port: Int) {

  //val server = "127.0.0.1:8081"
  val server = host + ":" + port

  // Note GWT uses old version of apache commons http client
  val params = new BasicHttpParams();
  //HttpProtocolParams.setVersion(params,HttpVersion.HTTP_1_1); 
  val sr = new SchemeRegistry()
  val http = new Scheme("http", new PlainSocketFactory(), 8081)
  sr.register(http)
  val cm = new ThreadSafeClientConnManager(params, sr)
  //val cm = new SingleClientConnManager(params,sr)
  // cm.createConnectionPool(100,50)
  //val cm = new ThreadSafeClientConnManager()
  //cm.setMaxTotal(500)
  //cm.setMaxDefaultPerRoute(500)
  //val cm = new ThreadSafeClientConnManager()
  val client = new DefaultHttpClient(cm, params)
  //  val client = new DefaultHttpClient(cm)

  private def check(code: Int, body: Json) {
    if (code != 200) {
      throw new SystemException(jgetString(body, "kind"), jget(body, "info"))
    }
  }

  var w: Window = null

  def setWindow(w: Window) { this.w = w }

  def wrap[T](default: => T)(body: => T): T = {
    try {
      body
    } catch {
      case ex: SystemException => throw ex
      case ex: Exception => {
        val notif = new Window.Notification("FATAL ERROR", ex.toString(), Window.Notification.TYPE_ERROR_MESSAGE)
        w.showNotification(notif)
        default
      }
    }
  }

  def unit {}

  def getDatabases(): Json = {
    wrap(Json("[]")) {
      val info = Source.fromURL(new URL("http://" + server + "/")).mkString
      Json(info)
    }
  }

  def getDatabaseStatus(databaseName: String): String = {
    wrap("unknown") {
      val info = Source.fromURL(new URL("http://" + server + "/" + databaseName + "?info&get=s")).mkString
      val jinfo = Json(info)
      jgetString(jinfo, "s")
    }
  }

  def startDatabase(databaseName: String) {
    wrap(unit) {
      val post = new HttpPost("http://" + server + "/" + databaseName)
      val e = new StringEntity("""{"cmd":"start"}""")
      post.setEntity(e)
      val response = client.execute(post)
      val e1 = response.getEntity()
      e1.consumeContent()
    }
  }

  def stopDatabase(databaseName: String) {
    wrap(unit) {
      val post = new HttpPost("http://" + server + "/" + databaseName)
      val e = new StringEntity("""{"cmd":"stop"}""")
      post.setEntity(e)
      val response = client.execute(post)
      val e1 = response.getEntity()
      e1.consumeContent()
    }
  }

  def configAct(cmd: String, databaseName: String, config: Json) {
    wrap(unit) {
      val post = new HttpPost("http://" + server + "/" + databaseName)
      val request = JsonObject("cmd" -> cmd, "config" -> config)
      val e = new StringEntity(Compact(request))
      post.setEntity(e)
      val response = client.execute(post)
      val e1 = response.getEntity()
      val code = response.getStatusLine().getStatusCode()
      e1.consumeContent()
      check(code, Json(getContent(e1)))
    }
  }

  def deleteDatabase(databaseName: String) {
    wrap(unit) {
      val post = new HttpPost("http://" + server + "/" + databaseName)
      val e = new StringEntity("""{"cmd":"delete"}""")
      post.setEntity(e)
      val response = client.execute(post)
      val e1 = response.getEntity()
      e1.consumeContent()
    }
  }

  private def getContent(e: HttpEntity): String = {
    val rd = new BufferedReader(new InputStreamReader(e.getContent()))
    val sb = new StringBuffer()
    var line = ""
    while (true) {
      val line = rd.readLine()
      if (line == null) {
        rd.close()
        return sb.toString()
      }
      sb.append(line + "\n")
    }
    ""
  }

  def getTables(databaseName: String): Json = {
    wrap(Json("[]")) {
      val get = new HttpGet("http://" + server + "/" + databaseName)
      val response = client.execute(get)
      val e1 = response.getEntity()
      val info = getContent(e1)
      Json(info)
    }
  }

  def getTableInfo(databaseName: String, tableName: String): Json = {
    wrap(Json("{}")) {
      val get = new HttpGet("http://" + server + "/" + databaseName + "/" + tableName + "?info&get=rtf")
      val response = client.execute(get)
      val e1 = response.getEntity()
      val info = getContent(e1)
      Json(info)
    }
  }

  def getKeyBatch(databaseName: String, tableName: String, count: Int,
    key: Option[JsonKey], includeKey: Boolean, first: Boolean,
    parentKey: Option[JsonKey]): JsonArray = {
    wrap(emptyJsonArray) {
      val ks = key match {
        case Some(k: Json) => {
          if (first) {
            "&low=" + keyUriEncode(k) + (if (includeKey) "&includelow" else "")

          } else {
            "&reverse&high=" + keyUriEncode(k) + (if (includeKey) "&includehigh" else "")
          }
        }
        case None => ""
      }
      val parent = parentKey match {
        case Some(k: Json) => "&parent=" + keyUriEncode(k)
        case None => ""
      }
      val info = Source.fromURL(new URL("http://" + server + "/" + databaseName + "/" + tableName + "?count=" + count + ks + parent)).mkString
      val items = jgetArray(Json(info))
      if (first) items else items.reverse
    }
  }

  def getRings(databaseName: String): Json = {
    wrap(Json("[]")) {
      val info = Source.fromURL(new URL("http://" + server + "/" + databaseName + "?rings")).mkString
      Json(info)
    }
  }

  def getNodes(databaseName: String, ringName: String): Json = {
    wrap(Json("[]")) {
      val info = Source.fromURL(new URL("http://" + server + "/" + databaseName + "/ring:" + ringName)).mkString
      Json(info)
    }
  }

  def getServers(databaseName: String): Json = {
    wrap(Json("[]")) {
      val info = Source.fromURL(new URL("http://" + server + "/" + databaseName + "?servers")).mkString
      Json(info)
    }
  }

  def getItem(databaseName: String, tableName: String, key: JsonKey): Json = {
    wrap(Json(null)) {
      val info = Source.fromURL(new URL("http://" + server + "/" + databaseName + "/" +
        tableName + "/" + keyUriEncode(key)) + "?get=vc").mkString
      Json(info)
    }
  }

  def putItem(databaseName: String, tableName: String, key: JsonKey, value: Json) {
    wrap(unit) {
      val put = new HttpPut("http://" + server + "/" + databaseName + "/" + tableName + "/" + keyUriEncode(key))
      val e = new StringEntity(Compact(value))
      put.setEntity(e)
      val response = client.execute(put)
      val e1 = response.getEntity()
      e1.consumeContent()
    }
  }

  def conditionalPutItem(databaseName: String, tableName: String, key: JsonKey, cv: Json, value: Json): Boolean = {
    wrap(false) {
      val put = new HttpPost("http://" + server + "/" + databaseName + "/" + tableName + "/" + keyUriEncode(key))
      val request = JsonObject("cmd" -> "put", "v" -> value, "c" -> cv)
      val e = new StringEntity(Compact(request))
      put.setEntity(e)
      val response = client.execute(put)
      val code = response.getStatusLine().getStatusCode()
      val e1 = response.getEntity()
      e1.consumeContent()
      code == 200
    }
  }

  def addItem(databaseName: String, tableName: String, key: JsonKey, value: Json): Boolean = {
    wrap(false) {
      val put = new HttpPut("http://" + server + "/" + databaseName + "/" + tableName + "/" + keyUriEncode(key) + "?create")
      val e = new StringEntity(Compact(value))
      put.setEntity(e)
      val response = client.execute(put)
      val code = response.getStatusLine().getStatusCode()
      // 200 versus 409
      val e1 = response.getEntity()
      e1.consumeContent()
      code == 200
    }
  }

  def deleteItem(databaseName: String, tableName: String, key: JsonKey) {
    wrap(unit) {
      val del = new HttpDelete("http://" + server + "/" + databaseName + "/" + tableName + "/" + keyUriEncode(key))
      val response = client.execute(del)
      val e1 = response.getEntity()
      e1.consumeContent()
    }
  }

  def conditionalDeleteItem(databaseName: String, tableName: String, key: JsonKey, cv: Json): Boolean = {
    wrap(false) {
      val del = new HttpPost("http://" + server + "/" + databaseName + "/" + tableName + "/" + keyUriEncode(key))
      val request = JsonObject("cmd" -> "delete", "c" -> cv)
      val e = new StringEntity(Compact(request))
      del.setEntity(e)
      val response = client.execute(del)
      val code = response.getStatusLine().getStatusCode()
      val e1 = response.getEntity()
      e1.consumeContent()
      code == 200
    }
  }

  def addTable(databaseName: String, tableName: String) {
    wrap(unit) {
      val del = new HttpPost("http://" + server + "/" + databaseName)
      val tableConfig = JsonObject("name" -> tableName)
      val config = JsonObject("tables" -> JsonArray(tableConfig))
      val request = JsonObject("cmd" -> "addTables", "config" -> config)
      val e = new StringEntity(Compact(request))
      del.setEntity(e)
      val response = client.execute(del)
      val code = response.getStatusLine().getStatusCode()
      val e1 = response.getEntity()
      e1.consumeContent()
    }
  }

  def deleteTable(databaseName: String, tableName: String) {
    wrap(unit) {
      val del = new HttpPost("http://" + server + "/" + databaseName)
      val tableConfig = JsonObject("name" -> tableName)
      val config = JsonObject("tables" -> JsonArray(tableConfig))
      val request = JsonObject("cmd" -> "deleteTables", "config" -> config)
      val e = new StringEntity(Compact(request))
      del.setEntity(e)
      val response = client.execute(del)
      val code = response.getStatusLine().getStatusCode()
      val e1 = response.getEntity()
      e1.consumeContent()
    }
  }
}