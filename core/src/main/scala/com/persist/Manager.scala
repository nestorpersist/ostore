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

// TODO multipart actions should occur as single step
// TODO lock and send actions only to ring involved (e.g.addNode)
// TODO deal with removal of the primary server

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
    private val request0 = JsonObject("guid" -> guid)
    private var servers = List[String]()

    private def doAll(cmd: String, request: JsonObject, servers1: List[String] = servers): Boolean = {
      val rs = Compact(request)
      var ok = true
      var futures = List[Future[Any]]()
      for (serverName <- servers1) {
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

    private def initialLock(expectedState: String, check: JsonObject): Boolean = {
      var request = request0 + ("getinfo" -> true)
      val tableName = jgetString(check, "table")
      if (tableName != "") request += ("table" -> tableName)
      val ringName = jgetString(check, "ring")
      if (ringName != "") request += ("ring" -> ringName)
      val nodeName = jgetString(check, "node")
      if (nodeName != "") request += ("node" -> nodeName)
      val f = server ? ("lock", databaseName, Compact(request))
      val (code, s: String) = Await.result(f, 5 seconds)
      if (code != Codes.Ok) {
        if (code == Codes.Locked) throw new Exception("Database: " + databaseName + " is locked")
        throw new Exception("Unknown lock failure: " + code)
      }
      val response = Json(s)
      val state = jgetString(response, "s")
      if (expectedState != "" && expectedState != state) {
        val f = server ? ("unlock", databaseName, Compact(request0))
        val (code, s: String) = Await.result(f, 5 seconds)
        throw new Exception("Bad state, expected " + expectedState + " actual" + state)
      }
      val serversArray = if (state == "none") {
        jgetArray(check, "servers")
      } else {
        jgetArray(response, "servers")
      }
      servers = List[String]()
      for (s<-serversArray) {
        val serverName = jgetString(s)
        servers = serverName :: servers
      }
      servers = servers.reverse
      var ex: Exception = null
      if (tableName != "") {
        val requestTableAbsent = jgetBoolean(check, "tableAbsent")
        val actualTableAbsent = jgetBoolean(response, "tableAbsent")
        if (requestTableAbsent) {
          if (!actualTableAbsent) {
            ex = new Exception("Table " + tableName + " already exists")
          }
        } else {
          if (actualTableAbsent) {
            ex = new Exception("Table " + tableName + " does not exist")
          }
        }
      }
      if (nodeName != "") {
        val requestNodeAbsent = jgetBoolean(check, "nodeAbsent")
        val actualNodeAbsent = jgetBoolean(response, "nodeAbsent") || jgetBoolean(response, "ringAbsent")
        if (requestNodeAbsent) {
          if (!actualNodeAbsent) {
            ex = new Exception("Node " + nodeName + " already exists")
          }
        } else {
          if (actualNodeAbsent) {
            ex = new Exception("Node " + nodeName + " does not exist")
          }
        }
      }
      if (nodeName == "" && ringName != "") {
        val requestRingAbsent = jgetBoolean(check, "ringAbsent")
        val actualRingAbsent = jgetBoolean(response, "ringAbsent")
        if (requestRingAbsent) {
          if (!actualRingAbsent) {
            ex = new Exception("Ring " + ringName + " already exists")
          }
        } else {
          if (actualRingAbsent) {
            ex = new Exception("Ring " + ringName + " does not exist")
          }
        }
      }
      if (ex != null) {
        val f = server ? ("unlock", databaseName, Compact(request0))
        val (code, s: String) = Await.result(f, 5 seconds)
        throw ex
      }
      val ok = doAll("lock", request0)
      if (!ok) finalUnlock()
      ok
    }

    private def finalUnlock() {
      doAll("unlock", request0)
    }

    def act(expectedState: String = "", check: JsonObject = emptyJsonObject)(body: => Unit):Boolean = {
      if (initialLock(expectedState, check)) {
        try {
          body
        } finally {
          finalUnlock()
        }
        true
      } else {
        false
      }
    }

    def lock(servers1:List[String])(body: => Unit):Boolean = {
      val ok = doAll("lock", request0, servers1)
      if (! ok) doAll("unlock", request0, servers1)
      if (ok) {
        try {
          body
        } finally {
          doAll("unlock", request0, servers1)
        }
      }
      ok
    }

    def pass(cmd: String, request:JsonObject=emptyJsonObject, servers:List[String]= List[String]()) {
      doAll(cmd, request)
    }

    def wait(cmd: String, request: JsonObject = emptyJsonObject) {
      for (i <- 1 until 60) { // try for 2 minutes
        val done = doAll(cmd, request)
        if (done) return
        Thread.sleep(2000) // 2 seconds
      }
      throw new Exception("Wait " + cmd + " timeout")
    }

    /*
    def hasServer(serverName: String): Boolean = {
      servers.contains(serverName)
    }
    */
    
    def getServers:List[String] = {
      servers
    }
  }

  private def allDatabases(): Iterable[String] = {
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

  private def allServers(databaseName: String): Iterable[String] = {
    val f = server ? ("allservers", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val servers = jgetArray(Json(s))
    var result = List[String]()
    for (server <- servers) {
      result = jgetString(server) +: result
    }
    result.reverse
  }

  private def allTables(databaseName: String): Iterable[String] = {
    val f = server ? ("alltables", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val tables = jgetArray(Json(s))
    var result = List[String]()
    for (table <- tables) {
      result = jgetString(table) +: result
    }
    result.reverse
  }

  private def allRings(databaseName: String): Iterable[String] = {
    val f = server ? ("allrings", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val rings = jgetArray(Json(s))
    var result = List[String]()
    for (ring <- rings) {
      result = jgetString(ring) +: result
    }
    result.reverse
  }

  private def allNodes(databaseName: String, ringName: String): Iterable[String] = {
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
      val result = databaseInfo(databaseName, JsonObject("info" -> "s"))
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
    val check = JsonObject("databaseAbsent" -> true, "servers" -> servers.reverse)
    val request = JsonObject("config" -> jconfig)
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
    val request = JsonObject("table" -> tableName)
    val request1 = request + ("tableAbsent" -> true)
    val dba = DatabaseActions(databaseName)
    dba.act("active", request1) {
      dba.pass("addTable1", request)
      dba.pass("addTable2", request)
    }
  }

  private def deleteTable(databaseName: String, tableName: String) {
    val request = JsonObject("table" -> tableName)
    val dba = DatabaseActions(databaseName)
    dba.act("active", request) {
      dba.pass("deleteTable1", request)
      dba.pass("deleteTable2", request)
    }
  }

  private def addNode(databaseName: String, ringName: String, nodeName: String, host: String, port: Int) {
    val request = JsonObject("ring" -> ringName, "node" -> nodeName)
    val request1 = request + ("nodeAbsent" -> true)
    val dba = DatabaseActions(databaseName)
    dba.act("active", request1) {
      val newServerName = host + ":" + port
      val newServer = sendServer.serverRef(newServerName)
      val oldServers = dba.getServers
      val creatingNewServer = !oldServers.contains(newServerName)
      val newServers = if (creatingNewServer) List(newServerName) else List[String]()

      dba.lock(newServers) {

        // Stop balancing
        dba.pass("stopBalance")
        dba.wait("busyBalance")

        // Adds node and its tables and update ring prev next connections on existing servers
        val request = JsonObject("ring" -> ringName, "node" -> nodeName, "host" -> host, "port" -> port)
        dba.pass("addNode", request)

        val info = databaseInfo(databaseName, JsonObject("info" -> "c"))
        val jconfig = jget(info, "c")
        val config = DatabaseConfig(databaseName, jconfig)

        // Create new server
        if (creatingNewServer) {
          val newRequest = JsonObject("config" -> jconfig)
          dba.pass("newDatabase", newRequest, newServers)
          //val f = newServer ? ("newDatabase", databaseName, Compact(newRequest))
          //Await.result(f, 5 seconds)
        }

        // Update table low/high settings
        val nextNodeName = config.rings(ringName).nextNodeName(nodeName)
        val prevNodeName = config.rings(ringName).prevNodeName(nodeName)
        val prevServerName = config.rings(ringName).nodes(prevNodeName).server.name
        val prevServer = sendServer.serverRef(prevServerName)
        val request1 = JsonObject("ring" -> ringName, "node" -> nodeName)
        for ((tableName, tableConfig) <- config.tables) {
          val request2 = request1 + ("table" -> tableName)
          val (low, high) = if (prevNodeName == nextNodeName) {
            ("\uFFFF", "")
          } else {
            val request3 = request2 + ("node" -> prevNodeName)
            val f = prevServer ? ("getLowHigh", databaseName, Compact(request3))
            val (code: String, s: String) = Await.result(f, 5 seconds)
            val response = Json(s)
            val prevLow = jgetString(response, "low")
            val prevHigh = jgetString(response, "high")
            (prevHigh, prevHigh)
          }
          val request3 = request2 + ("low" -> low, "high" -> high)
          val f2 = newServer ? ("setLowHigh", databaseName, Compact(request3))
          val (code: String, s: String) = Await.result(f2, 5 seconds)
        }

        // Start it all up
        if (creatingNewServer) {
          dba.pass("startDatabase2", request, newServers)
          //val f = newServer ? ("startDatabase2", databaseName, Compact(request))
          //Await.result(f, 5 seconds)
        }
        dba.pass("startBalance")
      }
    }
  }

  private def deleteNode(databaseName: String, ringName: String, nodeName: String) {
    val dba = DatabaseActions(databaseName)
    val request = JsonObject("ring" -> ringName, "node" -> nodeName)
    dba.act("active",request) {
      dba.pass("stopBalance", request)
      dba.wait("busyBalance")

      // Update config and relink prev next
      // and delete node (and if then empty, enclosing ring and database)
      dba.pass("deleteNode", request)

      dba.pass("startBalance")
      dba.pass("removeEmptyDatabase")
    }
  }

  private def addRing(databaseName: String, ringName: String, nodes: JsonArray) {
    val dba = DatabaseActions(databaseName)
    val request = JsonObject("ring" -> ringName, "ringAbsent" -> true)
    dba.act("active", request) {
      val oldServers = dba.getServers
      var newServers = List[String]()
      for (node <- nodes) {
        val nodeName = jgetString(node, "name")
        val host = jgetString(node, "host")
        val port = jgetInt(node, "port")
        val serverName = host + ":" + port
        if (!oldServers.contains(serverName)) {
          if (!newServers.contains(serverName)) {
            newServers = newServers :+ serverName
          }
        }
      }
      dba.lock(newServers) {
        val allServers = oldServers ::: newServers
        val info = databaseInfo(databaseName, JsonObject("info" -> "c"))
        val jconfig = jget(info, "c")
        val config = DatabaseConfig(databaseName, jconfig)
        val (fromRingName,fromRingConfig) = config.rings.head
        val newRequest = JsonObject("config" -> jconfig)
        dba.pass("newDatabase", newRequest, newServers)
        // ??? start

        dba.pass("stopBalance")
        dba.wait("busyBalance")

        // start all new nodes (with new config) state = building
        // set up copy acts and modify config (! available on new ring)
        val addRequest = JsonObject("ring" -> ringName, "from"-> fromRingName, "nodes"->nodes)
        dba.pass("addRing",addRequest, allServers)
        
        dba.pass("startBalance")
        // wait copy complete
        // set state to available (old and new servers)
      }

    }
  }

  // deleteRing
  // val request = JsonObject("ring" -> ringName)

  /**
   * Temporary debugging method.
   */
  def report(databaseName: String, tableName: String): Json = {
    var result = JsonObject()
    for (ringName <- allRings(databaseName)) {
      var ro = JsonObject()
      //val dest = Map("ring" -> ringName)
      // TODO could do calls in parallel
      for (nodeName <- allNodes(databaseName, ringName)) {
        val info = nodeInfo(databaseName, ringName, nodeName, JsonObject("info" -> "hp"))
        val host = jgetString(info, "h")
        val port = jgetInt(info, "p")
        implicit val timeout = Timeout(5 seconds)
        val tableRef: ActorRef = system.actorFor("akka://ostore@" + host + ":" + port +
          "/user/" + databaseName + "/" + ringName + "/" + nodeName + "/" + tableName)
        val f1 = tableRef ? ("report", 0L, "", "")
        val (code: String, uid: Long, x: String) = Await.result(f1, 5 seconds)
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
    case ("allDatabases", p: Promise[Iterable[String]]) => {
      complete(p) {
        allDatabases()
      }
    }
    case ("allTables", p: Promise[Iterable[String]], databaseName: String) => {
      complete(p) {
        allTables(databaseName)
      }
    }
    case ("allRings", p: Promise[Iterable[String]], databaseName: String) => {
      complete(p) {
        allRings(databaseName)
      }
    }
    case ("allNodes", p: Promise[Iterable[String]], databaseName: String, ringName: String) => {
      complete(p) {
        allNodes(databaseName, ringName)
      }
    }
    case ("allServers", p: Promise[Iterable[String]], databaseName: String) => {
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
    case ("addTables", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        for (t <- jgetArray(config, "tables")) {
          addTable(databaseName, jgetString(t, "name"))
        }
        Codes.Ok
      }
    }
    case ("deleteTables", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        for (t <- jgetArray(config, "tables")) {
          deleteTable(databaseName, jgetString(t, "name"))
        }
        Codes.Ok
      }
    }
    case ("addNodes", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        for (r <- jgetArray(config, "rings")) {
          val ringName = jgetString(r, "name")
          for (n <- jgetArray(r, "nodes")) {
            val nodeName = jgetString(n, "name")
            val host = jgetString(n, "host")
            val port = jgetInt(n, "port")
            addNode(databaseName, ringName, nodeName, host, port)
          }
        }
        Codes.Ok
      }
    }
    case ("deleteNodes", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        for (r <- jgetArray(config, "rings")) {
          val ringName = jgetString(r, "name")
          for (n <- jgetArray(r, "nodes")) {
            val nodeName = jgetString(n, "name")
            deleteNode(databaseName, ringName, nodeName)
          }
        }
        Codes.Ok
      }
    }
    case ("addRings", p: Promise[String], databaseName: String, config: Json) => {
      complete(p) {
        for (r <- jgetArray(config, "rings")) {
          val ringName = jgetString(r, "name")
          val nodes = jgetArray(r, "nodes")
          addRing(databaseName, ringName, nodes)
        }
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
        monitor(databaseName, tableName)
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