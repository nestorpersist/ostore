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

import akka.actor._
import java.text.DateFormat
import java.util.Date
import scala.io.Source
import Actor._
import JsonOps._
import java.io.InputStreamReader
import java.io.InputStream
import java.io.BufferedReader
import akka.pattern._
import scala.collection.immutable.HashMap
import scala.util.parsing.combinator._
import scala.collection.JavaConversions._
import java.net.URLDecoder
import akka.dispatch.Future
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.ExecutionContext
import akka.dispatch.Promise
import Exceptions._
import akka.dispatch.Await
import akka.pattern._
import com.typesafe.config.ConfigFactory

private[persist] class QParse extends JavaTokenParsers {
  private def decode(a: Any): String = {
    val s1 = a.toString()
    val s2 = URLDecoder.decode(s1, "UTF-8")
    s2
  }

  def q: Parser[JsonObject] =
    repsep(item, "&") ^^ { x => x.foldLeft(JsonObject())((a: JsonObject, b: JsonObject) => a ++ b) }

  def item: Parser[JsonObject] = (
    name ~ "=" ~ value ^^
    { case n ~ "=" ~ v => JsonObject(n -> v) }
    | name ^^ { n => JsonObject(n -> true) })

  def name: Parser[String] = """\w+""".r ^^ { x => decode(x) }

  def value: Parser[Json] = (
    "(" ~ ")" ^^ { _ => JsonArray() }
    | "(" ~ repsep(value, ",") ~ ")" ^^ { case "(" ~ v ~ ")" => v }
    | """["]""".r ~ """[^"]*""".r ~ """["]""".r ^^ { case q1 ~ v ~ q2 => v.toString() }
    | "true" ^^ { _ => true }
    | "false" ^^ { _ => false }
    | "null" ^^ { _ => null }
    | """[^)(,"&]*""".r ^^ { x =>
      val s = decode(x)
      try
        java.lang.Long.decode(s)
      catch {
        case ex: Exception => s
      }
    })
}

private[persist] object QParser extends QParse {
  def parse(s: String) = parseAll(q, s).get
}

/*
 * {
 *    "port":8081,
 *    "client":{
 *      "host":"hostname",
 *      "port":8010,
 *      "name":"rest"
 *    },
 *    "server":{
 *       "host":"hostname",
 *       "port":8011
 *    }
 * }
 */
private[persist] class RestClient(config:Json) extends HttpAction {

  private var port = jgetInt(config, "port")
  if (port == 0) port = 8081
  // TODO default client name to "rest"
  // TODO when associated with server, use server messaging
  // TODO make RestClient available as API and command line main

  val client = new Client(config)
  val system = client.system
  lazy implicit val ec = ExecutionContext.defaultExecutionContext(system)
  implicit val timeout = Timeout(5 seconds)

  val httpServer = system.actorOf(Props(new HttpServer(port, this, "rest")),name="@http")
  val f = httpServer ? ("start")
  Await.result(f, 5 seconds)
  
  private[persist] def stop() {
    val f = httpServer ? ("stop")
    Await.result(f, 5 seconds)
    system.stop(httpServer)
  }

  private def getOptions(s: String): JsonObject = {
    if (s == "") {
      JsonObject()
    } else {
      QParser.parse(s)
    }
  }

  private def listDatabases(): Future[Option[Json]] = {
    Future {
      var result = JsonArray()
      for (databaseName <- client.allDatabases) {
        result = databaseName +: result
      }
      Some(result.reverse)
    }
  }

  private def doGet(aclient: AsyncTable, key: JsonKey, options: JsonObject): Future[Option[Json]] = {
    val f = aclient.get(key, options)
    f map { v =>
      v match {
        case Some(j: Json) => Some(j)
        case None => {
          throw new SystemException(Codes.NoItem,
            JsonObject("database" -> aclient.databaseName, "table" -> aclient.tableName, "key" -> key))
        }
      }
    }
  }

  private def doPut(aclient: AsyncTable, key: JsonKey, cv: Json, value: Json, options: JsonObject): Future[Option[Json]] = {
    if (cv != null) {
      aclient.conditionalPut(key, value, cv, options - "update") map { success =>
        if (!success) {
          throw new SystemException(Codes.Conflict,
            JsonObject("database" -> aclient.databaseName, "table" -> aclient.tableName, "key" -> key))
        }
        None
      }
    } else if (jgetBoolean(options, "create")) {
      aclient.create(key, value, options - "create") map { success =>
        if (!success) {
          throw new SystemException(Codes.Conflict,
            JsonObject("database" -> aclient.databaseName, "table" -> aclient.tableName, "key" -> key))
        }
        None
      }
    } else {
      aclient.put(key, value, options) map { x =>
        None
      }
    }
  }

  private def doDelete(aclient: AsyncTable, key: JsonKey, cv: Json, value: Json, options: JsonObject): Future[Option[Json]] = {
    if (cv != null) {
      aclient.conditionalDelete(key, cv, options - "update") map { success =>
        if (!success) {
          throw new SystemException(Codes.Conflict,
            JsonObject("database" -> aclient.databaseName, "table" -> aclient.tableName, "key" -> key))
        }
        None
      }
    } else {
      aclient.delete(key, options) map { x =>
        None
      }
    }
  }

  private def databaseAct(databaseName: String, input: Json): Future[Option[Json]] = {
    Future {
      val cmd = jgetString(input, "cmd")
      val config = jget(input, "config")
      cmd match {
        case "create" => client.createDatabase(databaseName, config)
        case "delete" => client.deleteDatabase(databaseName)
        case "start" => client.startDatabase(databaseName)
        case "stop" => client.stopDatabase(databaseName)
        case "addTables" => client.database(databaseName).addTables(config)
        case "deleteTables" => client.database(databaseName).deleteTables(config)
        case "addNodes" => client.database(databaseName).addNodes(config)
        case "deleteNodes" => client.database(databaseName).deleteNodes(config)
        case "addRings" => client.database(databaseName).addRings(config)
        case "deleteRings" => client.database(databaseName).deleteRings(config)
        case x => {
          throw new SystemException(Codes.BadRequest, JsonObject("msg" -> "bad database post command", "cmd" -> x))
        }
      }
      None
    }
  }

  private def getDatabaseInfo(databaseName: String, options: JsonObject): Future[Option[Json]] = {
    Future {
      val result = client.database(databaseName).databaseInfo(options)
      Some(result)
    }
  }

  private def getTableInfo(database: Database, tableName: String, options: JsonObject): Future[Option[Json]] = {
    Future {
      val result = database.tableInfo(tableName, options)
      Some(result)
    }
  }

  private def getRingInfo(database: Database, ringName: String, options: JsonObject): Future[Option[Json]] = {
    Future {
      val result = database.ringInfo(ringName, options)
      Some(result)
    }
  }

  private def getServerInfo(database: Database, serverName: String, options: JsonObject): Future[Option[Json]] = {
    Future {
      val result = database.serverInfo(serverName, options)
      Some(result)
    }
  }

  private def getNodeInfo(database: Database, ringName: String, nodeName: String, options: JsonObject): Future[Option[Json]] = {
    Future {
      val result = database.nodeInfo(ringName, nodeName, options)
      Some(result)
    }
  }

  private def listTables(database: Database): Future[Option[Json]] = {
    Future {
      var result = JsonArray()
      for (tableName <- database.allTables) {
        result = tableName +: result
      }
      Some(result.reverse)
    }
  }

  private def listRings(database: Database): Future[Option[Json]] = {
    Future {
      var result = JsonArray()
      for (ringName <- database.allRings) {
        result = ringName +: result
      }
      Some(result.reverse)
    }
  }

  private def listServers(database: Database): Future[Option[Json]] = {
    Future {
      var result = JsonArray()
      for (serverName <- database.allServers) {
        result = serverName +: result
      }
      Some(result.reverse)
    }
  }

  private def listNodes(database: Database, ringName: String): Future[Option[Json]] = {
    Future {
      var result = JsonArray()
      for (nodeName <- database.allNodes(ringName)) {
        result = nodeName +: result
      }
      Some(result.reverse)
    }
  }

  private def monitor(database: Database, tableName: String): Future[Option[Json]] = {
    Future {
      val result = database.monitor(tableName)
      Some(result)
    }
  }

  private def report(database: Database, tableName: String): Future[Option[Json]] = {
    Future {
      val result = database.report(tableName)
      Some(result)
    }
  }

  private def search(database: Database, tableName: String, s: String): Future[Option[Json]] = {
    Future {
      val ts = new Text.TextSearch(database.table(tableName))
      Some(ts.find(s))
    }
  }

  private def doAll(count: Int, f: Future[Option[NextItem]], result: JsonArray): Future[Option[Json]] = {
    f flatMap {
      case Some(ni) => {
        val result1 = ni.value +: result
        if (count <= 1) {
          Future { Some(result1.reverse) }
        } else {
          doAll(count - 1, ni.next(), result1)
        }
      }
      case None => Future { Some(result.reverse) }
    }
  }

  private def listKeys(database: Database, tableName: String, options: JsonObject): Future[Option[Json]] = {
    val count = {
      val c = jgetInt(options, "count")
      if (c == 0) { 30 } else { c }
    }
    val all = database.asyncTable(tableName).all(options - "count")
    if (count == 0) {
      Promise.successful(Some(emptyJsonArray))
    } else {
      doAll(count, all, emptyJsonArray)
    }

  }

  def doAll(method: String, path: String, q: String, input: Json): (Boolean, Future[Option[Json]]) = {
    val opt = try {
      Some(getOptions(q))
    } catch {
      case ex => None
    }
    opt match {
      case Some(options: JsonObject) => {
        val isPretty = jgetBoolean(options, "pretty")
        try {
          val f = doAllParts(method, path, input, options - "pretty")
          (isPretty, f)
        } catch {
          case ex:SystemException => {
            (false, Promise.failed(ex))
          }
          case x => {
            (false, Promise.failed(InternalException("doAll:"+x.toString())))
          }
        }
      }
      case x => {
        (false, Promise.failed(RequestException("can't parse query string")))
      }
    }
  }

  private def doParts1(databaseName: String, method: String, input: Json, options: JsonObject): Future[Option[Json]] = {
    if (method == "post") {
      databaseAct(databaseName, input)
    } else {
      val info = jgetBoolean(options,"info")
      val rings = jgetBoolean(options, "rings")
      val servers = jgetBoolean(options, "servers")
      val check = JsonObject("skipCheck" -> true)
      if (rings) {
        val database = client.database(databaseName, check)
        listRings(database)
      } else if (servers) {
        val database = client.database(databaseName, check)
        listServers(database)
      } else if (info) {
        getDatabaseInfo(databaseName, options - "info")
      } else {
        // list tables
        val database = client.database(databaseName, check)
        listTables(database)
      }
    }
  }

  private def doParts2(database: Database, name: String, method: String, input: Json, options: JsonObject): Future[Option[Json]] = {
    val parts = name.split(":")
    val numParts = parts.size
    if (parts(0) == "server") {
      val sname = name.substring(parts(0).size + 1)
      val info = jgetBoolean(options, "info")
      if (info) {
        getServerInfo(database, sname, options - "info")
      } else {
        Promise.failed(RequestException("bad cmd"))
      }
    } else if (parts(0) == "ring" && numParts == 2) {
      val info = jgetBoolean("info")
      if (info) {
        getRingInfo(database, parts(1), options - "info")
      } else {
        val ringName = parts(1)
        listNodes(database, ringName)
      }
    } else if (numParts == 1) {
      val tableName = parts(0)
      val isMonitor = jgetBoolean(options, "monitor")
      val isReport = jgetBoolean(options, "report")
      val searchString = jgetString(options, "search")
      val info = jgetBoolean(options, "info")
      if (isMonitor) {
        monitor(database, tableName)
      } else if (isReport) {
        report(database, tableName)
      } else if (info) {
        getTableInfo(database, tableName, options - "info")
      } else if (searchString != "") {
        // Full text search test code
        search(database, tableName, searchString)
      } else {
        listKeys(database, tableName, options)
      }
    } else {
      Promise.failed(RequestException("bad cmd"))
    }

  }

  private def doParts3(database: Database, tableName: String, keyString: String, method: String, input: Json, options: JsonObject): Future[Option[Json]] = {
    val parts = tableName.split(":")
    val numParts = parts.size
    if (numParts == 2 && parts(0) == "ring") {
      getNodeInfo(database, parts(1), keyString, options)
    } else {
      var key = keyUriDecode(keyString)
      try {
        val asyncTable = database.asyncTable(tableName)
        if (method == "post") {
          // TODO verify c and v exist
          val cmd = jgetString(input, "cmd")
          val cv = jget(input, "c")
          cmd match {
            case "put" => {

              val v = jget(input, "v")
              doPut(asyncTable, key, cv, v, options)
            }
            case "delete" => doDelete(asyncTable, key, cv, input, options)
            case x => {
              Promise.failed(RequestException("bad cmd"))
            }
          }
        } else {
          method match {
            case "get" => doGet(asyncTable, key, options)
            case "put" => doPut(asyncTable, key, null, input, options)
            case "delete" => doDelete(asyncTable, key, null, input, options)
            case x => {
              val ex = new SystemException(Codes.BadRequest, JsonObject("msg" -> "bad method", "method" -> x))
              Promise.failed(ex)
            }
          }
        }
      } catch {
        case ex: Exception => {
          Promise.failed(ex)
        }
      }
    }
  }

  private def doAllParts(method: String, path: String, input: Json, options: JsonObject): Future[Option[Json]] = {
    val parts = path.split("/")
    val numParts = parts.length
    if (numParts == 1 && parts(0) == "") {
      listDatabases()
    } else {
      val databaseName = parts(0)
      if (numParts == 1) {
        doParts1(databaseName, method, input, options)
      } else {
        val tableName = parts(1)
        val database = client.database(databaseName, JsonObject("skipCheck" -> true))
        if (numParts == 2) {
          doParts2(database, tableName, method, input, options)
        } else if (numParts == 3) {
          val keyString = parts(2)
          doParts3(database, tableName, keyString, method, input, options)
        } else {
          Promise.failed(RequestException("Bad url form"))
        }
      }
    }
  }
}
