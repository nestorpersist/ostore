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
import akka.util.Timeout
import akka.pattern._
import akka.util.duration._
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.Promise
import akka.actor.Props
import akka.dispatch.DefaultPromise
import akka.actor.ActorRef

class Manager(host: String, port: Int) extends CheckedActor {
  private val system = context.system
  private implicit val timeout = Timeout(5 seconds)
  private implicit val executor = system.dispatcher

  private val sendServer = new SendServer(system)

  private[persist] class DbInfo(val databaseName: String, val config: DatabaseConfig, var status: String) {
    private var sendRef:ActorRef = null
    def send = {
      if (sendRef == null) {
        sendRef = system.actorOf(Props(new Send(system,config)))
        val f = sendRef ? ("start")
        Await.result(f,5 seconds)
      }
      sendRef
    }
    def stop = {
      if (sendRef != null) {
        val f = sendRef ? ("stop")
        Await.result(f, 5 seconds)
        val f1 = gracefulStop(sendRef, 5 seconds)(system)
        Await.result(f1, 5 seconds)
        sendRef = null
      }
    }
    lazy val database = new Database(system, databaseName, self)
    // TODO cache syncTable and asyncTable values ???
    // could also be used to check table exists
    def syncTable(tableName:String) = new SyncTable(databaseName, tableName, asyncTable(tableName))
    def asyncTable(tableName:String) = new AsyncTable(databaseName, tableName, system, send)
  }

  // TODO don't pass to RestClient
  private var databases = new TreeMap[String, DbInfo]()

  private val server = sendServer.serverRef(host + ":" + port)
  private val f = server ? ("databases")
  private val (code: String, s: String) = Await.result(f, 5 seconds)
  private val dblist = jgetArray(Json(s))
  for (db <- dblist) {
    val databaseName = jgetString(db, "name")
    // TODO make sure name not already present
    val state = jgetString(db, "state")
    val f = server ? ("database", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val config = Json(s)
    val conf = DatabaseConfig(databaseName, config)
    addDatabase(databaseName, conf, state)
  }

  private def stop() {
    for ((databaseName, info) <- databases) {
      info.stop
    }
  }

  // TODO get from server
  private def allDatabases(): Traversable[String] = databases.keys

  // TODO get from server
  private def databaseInfo(databaseName: String, options: JsonObject = emptyJsonObject): Json = {
    JsonObject("status" -> getDatabaseStatus(databaseName))
  }

  private def allServers(databaseName: String) = databases(databaseName).config.servers.keys
  
  private def allTables(databaseName: String) = databases(databaseName).config.tables.keys
  
  private def allRings(databaseName: String) = databases(databaseName).config.rings.keys
  
  private def allNodes(databaseName:String, ringName: String) = databases(databaseName).config.rings(ringName).nodes.keys

  private def getDatabaseStatus(databaseName: String): String = {
    // TODO get status from server???
    if (!databases.contains(databaseName)) {
      "none"
    } else {
      databases(databaseName).status
    }
  }

  private def setDatabaseStatus(databaseName: String, status: String) {
    databases(databaseName).status = status
  }

  private def addDatabase(databaseName: String, config: DatabaseConfig, status: String) {
    val info = new DbInfo(databaseName, config, status)
    databases += (databaseName -> info)
  }

  private def removeDatabase(databaseName: String) {
    databases(databaseName).stop
    databases -= databaseName
  }

  private def createDatabase(databaseName: String, config: Json) {
    val conf = DatabaseConfig(databaseName, config)
    var ok = true
    // TODO could be done in parallel
    for (serverName <- conf.servers.keys) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("newDatabase", databaseName, Compact(config))
      val v = Await.result(f, 5 seconds)
      if (v != Codes.Ok) {
        ok = false
        //println("Create failed: " +v + ":" + serverInfo.name) 
      }
    }
    if (ok) {
      addDatabase(databaseName, conf, "active")
      for (serverName <- conf.servers.keys) {
        val server = sendServer.serverRef(serverName)
        val f = server ? ("startDatabase2", databaseName)
        val v = Await.result(f, 5 seconds)
      }
    }
  }

  private def deleteDatabase(databaseName: String) {
    val status = getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "stop") throw new Exception("database not stopped")
    // TODO could be done in parallel
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("deleteDatabase", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    removeDatabase(databaseName)
  }

  private def startDatabase(databaseName: String) {
    val status = getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "stop") throw new Exception("database not stopped")
    // TODO could be done in parallel
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("startDatabase1", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("startDatabase2", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    setDatabaseStatus(databaseName, "active")
  }

  private def stopDatabase(databaseName: String) {
    val status = getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "active") throw new Exception("database not active")
    // TODO could be done in parallel
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("stopDatabase1", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("stopDatabase2", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    setDatabaseStatus(databaseName, "stop")
  }
  
    
  /**
   * Temporary debugging method.
   */
  def report(databaseName: String, tableName:String): Json = {
    var result = JsonObject()
    for (ringName <- allRings(databaseName)) {
      var ro = JsonObject()
      val dest = Map("ring" -> ringName)
      // TODO could do calls in parallel
      for (nodeName <- allNodes(databaseName,ringName)) {
        val dest1 = dest + ("node" -> nodeName)
        var f1 = new DefaultPromise[Any]
        databases(databaseName).send ! ("report", dest1, f1, tableName, "", "")
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
  def monitor(databaseName: String, tableName:String): Json = {
    var result = JsonObject()
    for (ringName <- allRings(databaseName)) {
      var ro = JsonObject()
      // TODO could do calls in parallel
      for ((nodeName,nodeConfig) <- databases(databaseName).config.rings(ringName).nodes) {
        implicit val timeout = Timeout(5 seconds)
        val monitorRef: ActorRef = system.actorFor("akka://ostore@" + nodeConfig.server.host + ":" + nodeConfig.server.port + 
            "/user/" + databases(databaseName).config.name + "/" + ringName + "/" + nodeName + "/@mon")
        val f1 = monitorRef ? ("get", tableName)
        val (code: String, x: String) = Await.result(f1, 5 seconds)
        ro = ro + (nodeName -> Json(x))
      }
      result = result + (ringName -> ro)
    }
    result
  }
  

  // TODO check database exists
  private def database(databaseName: String) = databases(databaseName).database

  private def complete[T](p: Promise[T])(x: => T) {
    val v = try {
      Right(x)
    } catch {
      case t: Throwable => Left(t)
    }
    p.complete(v)
  }

  def rec = {
    case ("database", p: Promise[(String, Database)], databaseName: String) => {
      complete(p) {
        // TODO make sure database exists
        (Codes.Ok, database(databaseName))
      }
    }
    case ("syncTable", p: Promise[SyncTable], databaseName: String, tableName:String) => {
      complete(p) {
        databases(databaseName).syncTable(tableName)
      }
    }
    case ("asyncTable", p: Promise[AsyncTable], databaseName: String, tableName:String) => {
      complete(p) {
        databases(databaseName).asyncTable(tableName)
      }
    }
    case ("allDatabases", p: Promise[Traversable[String]]) => {
      complete(p) {
        allDatabases()
      }
    }
    case ("allTables", p: Promise[Traversable[String]], databaseName:String) => {
      complete(p) {
        allTables(databaseName)
      }
    }
    case ("allRings", p: Promise[Traversable[String]], databaseName:String) => {
      complete(p) {
        allRings(databaseName)
      }
    }
    case ("allNodes", p: Promise[Traversable[String]], databaseName:String, ringName:String) => {
      complete(p) {
        allNodes(databaseName, ringName)
      }
    }
    case ("allServers", p: Promise[Traversable[String]], databaseName:String) => {
      complete(p) {
        allServers(databaseName)
      }
    }
    case ("databaseInfo", p: Promise[Json], databaseName:String) => {
      complete(p) {
        databaseInfo(databaseName)
      }
    }
    case ("createDatabase", p: Promise[String], databaseName: String, config: Json) => {
    //case ("createDatabase", p: Any, databaseName: String, config: Json) => {
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
    case ("addTable", p: Promise[String], databaseName: String, tableName:String) => {
      complete(p) {
        // TODO
        // TODO update local config and map
        println("Add table: "+ databaseName +":" + tableName)
        Codes.Ok
      }
    }
    case ("deleteTable", p: Promise[String], databaseName: String, tableName:String) => {
      complete(p) {
        // TODO
        // TODO update local config and map
        println("Delete table: "+ databaseName +":" + tableName)
        Codes.Ok
      }
    }
    case ("report", p: Promise[Json], databaseName: String, tableName:String) => {
      complete(p) {
        report(databaseName, tableName)
      }
    }
    case ("monitor", p: Promise[Json], databaseName: String, tableName:String) => {
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