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

import akka.actor.ActorSystem
import scala.collection.immutable.TreeMap
import JsonOps._
import Codes._
import akka.util.Timeout
import akka.pattern._
import akka.util.duration._
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.Promise
import akka.actor.Props
import akka.dispatch.DefaultPromise
import akka.actor.ActorRef
import java.util.UUID

class Manager(host: String, port: Int) extends CheckedActor {
  private val system = context.system
  private implicit val timeout = Timeout(5 seconds)
  private implicit val executor = system.dispatcher

  private val sendServer = new SendServer(system)
  private val server = sendServer.serverRef(host + ":" + port)

  private var sends = new TreeMap[String, ActorRef]()
  private var databases = new TreeMap[String, Database]()

  private def getAsyncTable(databaseName: String, tableName: String): AsyncTable = {
    val send = sends.get(databaseName) match {
      case Some(s) => s
      case None => {
        val info = databaseInfo(databaseName, JsonObject("info" -> "c"))
        val jconfig = jget(info, "c")
        val config = DatabaseConfig(databaseName, jconfig)
        val s = system.actorOf(Props(new Send(system, config)))
        val f = s ? ("start")
        Await.result(f, 5 seconds)
        sends += (databaseName -> s)
        s
      }
    }
    new AsyncTable(databaseName, tableName, system, send)
  }

  private def getSyncTable(databaseName: String, tableName: String): SyncTable = {
    new SyncTable(databaseName, tableName, getAsyncTable(databaseName, tableName))
  }

  private def getDatabase(databaseName: String): Database = {
    databases.get(databaseName) match {
      case Some(d) => d
      case None => {
        val d = new Database(system, databaseName, self)
        databases += (databaseName -> d)
        d
      }
    }
  }

  private def stop() {
    for ((databaseName, sendRef) <- sends) {
      val f = sendRef ? ("stop")
      Await.result(f, 5 seconds)
      val f1 = gracefulStop(sendRef, 5 seconds)(system)
      Await.result(f1, 5 seconds)
    }
  }

  private case class DatabaseActions(databaseName: String) {
    
    private val guid = UUID.randomUUID().toString()
    private val request0 = JsonObject(("guid"->guid))
    private var servers:JsonArray = emptyJsonArray

    private def doAll(cmd:String, request:JsonObject):Boolean = {
      val rs = Compact(request)
      var ok = true
      var futures = List[Future[Any]]()
      for (serverj <- servers) {
        val serverName = jgetString(serverj)
        val server = sendServer.serverRef(serverName)
        val f = server ? (cmd, databaseName, rs)
        futures = f +: futures
      }
      for (f <- futures.reverse) {
        val (code, response) = Await.result(f, 5 seconds)
        if (code != Codes.Ok) ok = false;
      }
      ok
    }

    private def lock(expectedState:String, check:JsonObject):Boolean = {
      var request = request0 + ("getinfo"->true)
      val tableName = jgetString(check, "table")
      if (tableName != "") request += ("table" -> tableName)
      val f = server ? ("lock", databaseName, Compact(request))
      val (code, s:String) = Await.result(f, 5 seconds)
      if (code != Codes.Ok) {
        if (code == Codes.Locked) throw new Exception("Database: " + databaseName + " is locked")
        throw new Exception("Unknown lock failure: " + code)
      }
      val response = Json(s)
      val state = jgetString(response, "s")
      if (expectedState != "" && expectedState != state) {
        val f = server ? ("unlock", databaseName, Compact(request0))
        val (code, s:String) = Await.result(f, 5 seconds)
        throw new Exception("Bad state, expected " + expectedState + " actual" + state)
      }
      servers = if (state == "none") {
         jgetArray(check, "servers")
      } else {
         jgetArray(response,"servers")
      }
      if (tableName != "") {
        val requestTableAbsent = jgetBoolean(check, "tableAbsent")
        val actualTableAbsent = jgetBoolean(response, "tableAbsent")
        if (requestTableAbsent) {
          if (! actualTableAbsent) {
            val f = server ? ("unlock", databaseName, Compact(request0))
            val (code, s:String) = Await.result(f, 5 seconds)
            throw new Exception("Table " + tableName + " already exists")
          }
        } else {
          if (actualTableAbsent) {
            val f = server ? ("unlock", databaseName, Compact(request0))
            val (code, s:String) = Await.result(f, 5 seconds)
            throw new Exception("Table " + tableName + " does not exist")
          }
          
        }
      }
      val ok = doAll("lock", request0)
      if (! ok) unlock()
      ok
    }
    
    private def unlock() {
      doAll("unlock", request0)
    }
    
    def act(expectedState:String = "", check:JsonObject = emptyJsonObject)(body: => Unit) = {
      if (lock(expectedState, check)) {
        body
        unlock()
        true
      } else {
        false
      }
    }

    def pass(cmd:String, request:JsonObject = emptyJsonObject) {
      doAll(cmd, request)
    }
  }

  private def allDatabases(): Traversable[String] = {
    val f = server ? ("allDatabases")
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val databases = jgetArray(Json(s))
    var result = List[String]()
    for (database <- databases) {
      result = jgetString(database) +: result
    }
    result.reverse
  }

  private def databaseInfo(databaseName: String, options: JsonObject): Json = {
    val f = server ? ("databaseinfo", databaseName, Compact(options))
    val (code: String, s: String) = Await.result(f, 5 seconds)
    if (code == Codes.Ok) {
      Json(s)
    } else {
      throw new Exception(s)
    }
  }

  private def tableInfo(databaseName: String, tableName: String, options: JsonObject): Json = {
    val f = server ? ("tableinfo", databaseName, tableName, Compact(options))
    val (code: String, s: String) = Await.result(f, 5 seconds)
    Json(s)
  }

  private def serverInfo(databaseName: String, serverName: String, options: JsonObject): Json = {
    val f = server ? ("serverinfo", databaseName, serverName, Compact(options))
    val (code: String, s: String) = Await.result(f, 5 seconds)
    Json(s)
  }

  private def ringInfo(databaseName: String, ringName: String, options: JsonObject): Json = {
    val f = server ? ("ringinfo", databaseName, ringName, Compact(options))
    val (code: String, s: String) = Await.result(f, 5 seconds)
    Json(s)
  }
  private def nodeInfo(databaseName: String, ringName: String, nodeName: String, options: JsonObject): Json = {
    val f = server ? ("nodeinfo", databaseName, ringName, nodeName, Compact(options))
    val (code: String, s: String) = Await.result(f, 5 seconds)
    Json(s)
  }

  private def allServers(databaseName: String): Traversable[String] = {
    val f = server ? ("allservers", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val servers = jgetArray(Json(s))
    var result = List[String]()
    for (server <- servers) {
      result = jgetString(server) +: result
    }
    result.reverse
  }

  private def allTables(databaseName: String): Traversable[String] = {
    val f = server ? ("alltables", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val tables = jgetArray(Json(s))
    var result = List[String]()
    for (table <- tables) {
      result = jgetString(table) +: result
    }
    result.reverse
  }

  private def allRings(databaseName: String): Traversable[String] = {
    val f = server ? ("allrings", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val rings = jgetArray(Json(s))
    var result = List[String]()
    for (ring <- rings) {
      result = jgetString(ring) +: result
    }
    result.reverse
  }

  private def allNodes(databaseName: String, ringName: String): Traversable[String] = {
    val f = server ? ("allnodes", databaseName, ringName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val nodes = jgetArray(Json(s))
    var result = List[String]()
    for (node <- nodes) {
      result = jgetString(node) +: result
    }
    result.reverse
  }

  private def getDatabaseStatus(databaseName: String): String = {
    try {
      val result = databaseInfo(databaseName, JsonObject(("info" -> "s")))
      jgetString(result, "s")
    } catch {
      case ex => "none"
    }
  }

  private def createDatabase(databaseName: String, jconfig: Json) {
    val dba = DatabaseActions(databaseName)
    val config = DatabaseConfig(databaseName, jconfig)
    var servers = JsonArray()
    for ((serverName, serverConfig) <- config.servers) {
      servers = serverName +: servers
    }
    val check = JsonObject(("databaseAbsent" -> true),("servers" -> servers.reverse))
    val request = JsonObject(("config" -> jconfig))
    dba.act("none", check) {
      dba.pass("newDatabase", request)
      dba.pass("startDatabase2")
    }
  }

  private def deleteDatabase(databaseName: String) {
    val dba = DatabaseActions(databaseName)
    dba.act("stop") {
      dba.pass("deleteDatabase")
    }
  }

  private def startDatabase(databaseName: String) {
    val dba = DatabaseActions(databaseName)
    dba.act("stop") {
      dba.pass("startDatabase1")
      dba.pass("startDatabase2")
    }
  }

  private def stopDatabase(databaseName: String) {
    val dba = DatabaseActions(databaseName)
    dba.act("active") {
      dba.pass("stopDatabase1")
      dba.pass("stopDatabase2")
    }
  }

  private def addTable(databaseName: String, tableName: String) {
    val request = JsonObject(("table"->tableName))
    val result1 = request + ("tableAbsent"->true)
    val dba = DatabaseActions(databaseName)
    dba.act("active") {
      dba.pass("addTable1", request)
      dba.pass("addTable2", request)
    }
  }

  private def deleteTable(databaseName: String, tableName: String) {
    val request = JsonObject(("table"->tableName))
    val dba = DatabaseActions(databaseName)
    dba.act("active") {
      dba.pass("deleteTable1", request)
      dba.pass("deleteTable2", request)
    }
  }

  /**
   * Temporary debugging method.
   */
  def report(databaseName: String, tableName: String): Json = {
    var result = JsonObject()
    for (ringName <- allRings(databaseName)) {
      var ro = JsonObject()
      val dest = Map("ring" -> ringName)
      // TODO could do calls in parallel
      for (nodeName <- allNodes(databaseName, ringName)) {
        val dest1 = dest + ("node" -> nodeName)
        var f1 = new DefaultPromise[Any]
        sends(databaseName) ! ("report", dest1, f1, tableName, "", "")
        val (code: String, x: String) = Await.result(f1, 5 seconds)
        ro = ro + (nodeName -> Json(x))
      }
      result = result + (ringName -> ro)
    }
    result
  }

  /**
   * Temporary debugging method
   */
  def monitor(databaseName: String, tableName: String): Json = {
    var result = JsonObject()
    for (ringName <- allRings(databaseName)) {
      var ro = JsonObject()
      // TODO could do calls in parallel
      for (nodeName <- allNodes(databaseName, ringName)) {
        val info = nodeInfo(databaseName, ringName, nodeName, JsonObject("info" -> "hp"))
        val host = jgetString(info, "h")
        val port = jgetInt(info, "p")
        implicit val timeout = Timeout(5 seconds)
        val monitorRef: ActorRef = system.actorFor("akka://ostore@" + host + ":" + port +
          "/user/" + databaseName + "/" + ringName + "/" + nodeName + "/@mon")
        val f1 = monitorRef ? ("get", tableName)
        val (code: String, x: String) = Await.result(f1, 5 seconds)
        ro = ro + (nodeName -> Json(x))
      }
      result = result + (ringName -> ro)
    }
    result
  }

  private def complete[T](p: Promise[T])(x: => T) {
    val v = try {
      Right(x)
    } catch {
      case t: Throwable => Left(t)
    }
    p.complete(v)
  }

  def rec = {
    case ("database", p: Promise[Database], databaseName: String) => {
      complete(p) {
        getDatabase(databaseName)
      }
    }
    case ("syncTable", p: Promise[SyncTable], databaseName: String, tableName: String) => {
      complete(p) {
        getSyncTable(databaseName, tableName)
      }
    }
    case ("asyncTable", p: Promise[AsyncTable], databaseName: String, tableName: String) => {
      complete(p) {
        getAsyncTable(databaseName, tableName)
      }
    }
    case ("allDatabases", p: Promise[Traversable[String]]) => {
      complete(p) {
        allDatabases()
      }
    }
    case ("allTables", p: Promise[Traversable[String]], databaseName: String) => {
      complete(p) {
        allTables(databaseName)
      }
    }
    case ("allRings", p: Promise[Traversable[String]], databaseName: String) => {
      complete(p) {
        allRings(databaseName)
      }
    }
    case ("allNodes", p: Promise[Traversable[String]], databaseName: String, ringName: String) => {
      complete(p) {
        allNodes(databaseName, ringName)
      }
    }
    case ("allServers", p: Promise[Traversable[String]], databaseName: String) => {
      complete(p) {
        allServers(databaseName)
      }
    }
    case ("databaseInfo", p: Promise[Json], databaseName: String, options: JsonObject) => {
      complete(p) {
        databaseInfo(databaseName, options)
      }
    }
    case ("tableInfo", p: Promise[Json], databaseName: String, tableName: String, options: JsonObject) => {
      complete(p) {
        tableInfo(databaseName, tableName, options)
      }
    }
    case ("ringInfo", p: Promise[Json], databaseName: String, ringName: String, options: JsonObject) => {
      complete(p) {
        ringInfo(databaseName, ringName, options)
      }
    }
    case ("serverInfo", p: Promise[Json], databaseName: String, serverName: String, options: JsonObject) => {
      complete(p) {
        serverInfo(databaseName, serverName, options)
      }
    }
    case ("nodeInfo", p: Promise[Json], databaseName: String, ringName: String, nodeName: String, options: JsonObject) => {
      complete(p) {
        nodeInfo(databaseName, ringName, nodeName, options)
      }
    }
    case ("createDatabase", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        createDatabase(databaseName, config)
        Codes.Ok
      }
    }
    case ("startDatabase", p: Promise[String], databaseName: String) => {
      complete(p) {
        startDatabase(databaseName)
        Codes.Ok
      }
    }
    case ("stopDatabase", p: Promise[String], databaseName: String) => {
      complete(p) {
        stopDatabase(databaseName)
        Codes.Ok
      }
    }
    case ("deleteDatabase", p: Promise[String], databaseName: String) => {
      complete(p) {
        deleteDatabase(databaseName)
        Codes.Ok
      }
    }
    case ("addTable", p: Promise[String], databaseName: String, tableName: String) => {
      complete(p) {
        addTable(databaseName, tableName)
        Codes.Ok
      }
    }
    case ("deleteTable", p: Promise[String], databaseName: String, tableName: String) => {
      complete(p) {
        deleteTable(databaseName, tableName)
        Codes.Ok
      }
    }
    case ("addTables", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        //addTabless(databaseName, config)
        println("Add tables: " + databaseName)
        println(Pretty(config))
        Codes.Ok
      }
    }
    case ("deleteTables", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        //deleteNodes(databaseName, config)
        println("Delete tables: " + databaseName)
        println(Pretty(config))
        Codes.Ok
      }
    }
    case ("addNodes", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        //addNodes(databaseName, config)
        println("Add nodes: " + databaseName)
        println(Pretty(config))
        Codes.Ok
      }
    }
    case ("deleteNodes", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        //deleteNodes(databaseName, config)
        println("Delete nodes: " + databaseName)
        println(Pretty(config))
        Codes.Ok
      }
    }
    case ("addRings", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        //addNodes(databaseName, config)
        println("Add rings: " + databaseName)
        println(Pretty(config))
        Codes.Ok
      }
    }
    case ("deleteRings", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        //deleteNodes(databaseName, config)
        println("Delete rings: " + databaseName)
        println(Pretty(config))
        Codes.Ok
      }
    }
    case ("report", p: Promise[Json], databaseName: String, tableName: String) => {
      complete(p) {
        report(databaseName, tableName)
      }
    }
    case ("monitor", p: Promise[Json], databaseName: String, tableName: String) => {
      complete(p) {
        report(databaseName, tableName)
      }
    }
    case ("stop", p: Promise[String]) => {
      complete(p) {
        stop()
        Codes.Ok
      }
    }
  }
}