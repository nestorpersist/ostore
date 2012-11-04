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

import JsonOps._
import Codes.emptyResponse
import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch._
import akka.remote.RemoteClientError
import akka.remote.RemoteClientWriteFailed
import akka.actor.DeadLetter
import akka.remote.RemoteLifeCycleEvent
import scala.collection.immutable.TreeMap
import java.io.File
import scala.io.Source
import akka.event.Logging
import com.typesafe.config.ConfigValueFactory
import Stores._

// Server Actor Paths
//       /user/@server
//       /user/database
//       /user/database/@send
//       /user/database/ring
//       /user/database/ring/node
//       /user/database/ring/node/@mon
//       /user/database/ring/node/table

private class Listener extends CheckedActor {
  def rec = {
    case r: RemoteClientError => {
      println("*****Remote Client Error:" + r.getCause().getMessage() + ":" + r.getRemoteAddress())
    }
    case w: RemoteClientWriteFailed => {
      println("*****Remote Write Failed:" + w.getRequest() + ":" + w.getRemoteAddress())
    }
    case x => //println("*****Other Event:" + x)
  }
}

private[persist] class ServerActor(serverConfig: Json, create: Boolean) extends CheckedActor {
  private implicit val timeout = Timeout(5 seconds)

  private def deletePath(f: File) {
    if (f.isDirectory()) {
      for (f1: File <- f.listFiles()) {
        deletePath(f1)
      }
    }
    f.delete()
  }

  private val akkaConfig = ConfigFactory.load
  private var akkaClientConfig = akkaConfig.getConfig("client")
  private val akkaServerConfig = akkaConfig.getConfig("server")

  private var host = jgetString(serverConfig, "host")
  if (host == "") host = akkaServerConfig.getString("akka.remote.netty.hostname")
  private var port = jgetInt(serverConfig, "port")
  if (port == 0) port = akkaServerConfig.getInt("akka.remote.netty.port")
  private val serverName = host + ":" + port

  val store = Store("@server" + "/" + serverName, jget(serverConfig, "store"), context, create)
  private val system = context.system
  private val sendServer = new ManagerMessaging(system)
  private val listener = system.actorOf(Props[Listener])

  system.eventStream.subscribe(self, classOf[DeadLetter])
  system.eventStream.subscribe(listener, classOf[RemoteLifeCycleEvent])

  private var states = new DatabaseStates(store)

  private val restInfo = jget(serverConfig, "rest")
  private var restClient: RestClient = null
  if (restInfo != null) {
    var httpPort = jgetInt(restInfo, "port")
    if (httpPort == 0) httpPort = 8081
    var clientPort = jgetInt(restInfo, "client", "port")
    if (clientPort == 0) clientPort = 8012
    var clientName = jgetString(restInfo, "client", "name")
    if (clientName == "") clientName = "rest"
    val clientConfig = JsonObject(
      "port" -> httpPort,
      "client" -> JsonObject(
        "host" -> host,
        "port" -> clientPort,
        "name" -> clientName),
      "server" -> JsonObject(
        "host" -> host,
        "port" -> port))
    restClient = new RestClient(clientConfig)
  }

  if (!store.created) states.restore

  private def doLock(databaseName: String, request: Json): (String, String) = {
    val getInfo = jgetBoolean(request, "getinfo")
    val tableName = jgetString(request, "table")
    val ringName = jgetString(request, "ring")
    val nodeName = jgetString(request, "node")
    states.get(databaseName) match {
      case Some(state) => {
        val result = if (getInfo && state.config != null) {
          var servers = JsonArray()
          for (name <- state.config.servers.keys) {
            servers = name +: servers
          }
          var result = JsonObject("servers" -> servers.reverse, "s" -> state.state)
          if (tableName != "") {
            state.config.tables.get(tableName) match {
              case Some(tinfo) =>
              case None => {
                result += ("tableAbsent" -> true)
              }
            }
          }
          if (ringName != "") {
            state.config.rings.get(ringName) match {
              case Some(tinfo) => {
                if (nodeName != "") {
                  tinfo.nodes.get(nodeName) match {
                    case Some(ninfo) =>
                    case None => {
                      result += ("nodeAbsent" -> true)
                    }
                  }
                }
              }
              case None => {
                result += ("ringAbsent" -> true)
              }
            }
          }
          result
        } else {
          if (getInfo && state.config == null) {
            JsonObject("databaseAbsent" -> true)
          } else {
            emptyJsonObject
          }
        }
        (Codes.Ok, Compact(result))
      }
      case None => {
        var result = JsonObject("databaseAbsent" -> true, "s" -> "none")
        (Codes.Ok, Compact(result))
      }
    }
  }

  def recAdmin(cmd: String, databaseName: String, state: DatabaseState, request: Json) = {
    val database = state.ref
    cmd match {
      case "start" => {
        val balance = jgetBoolean(request, "balance")
        val user = jgetBoolean(request, "user")
        val ring = jgetString(request, "ring")
        val node = jgetString(request, "node")
        val f = database ? ("start", ring, node, balance, user)
        Await.result(f, 5 seconds)
        if (user) {
          state.state = DBState.ACTIVE
          log.info("Database started " + databaseName)
        }
        sender ! (Codes.Ok, emptyResponse)
      }
      case "stop" => {
        val user = jgetBoolean(request, "user")
        val balance = jgetBoolean(request, "balance")
        val forceRing = jgetString(request, "ring")
        val forceNode = jgetString(request, "node")
        val f = database ? ("stop", user, balance, forceRing, forceNode)
        Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
      }
      case "setRingAvailable" => {
        val ringName = jgetString(request, "ring")
        val avail = jgetBoolean(request, "avail")
        val config = state.config.enableRing(ringName, avail)
        // TODO enable user ops on new ring node table nodes
        sender ! (Codes.Ok, emptyResponse)
      }
      case "busySend" => {
        val f = database ? ("busySend")
        val (code: String, result: String) = Await.result(f, 5 seconds)
        sender ! (code, emptyResponse)
      }
      case "busyBalance" => {
        val f = database ? ("busyBalance")
        val (code: String, result: String) = Await.result(f, 5 seconds)
        sender ! (code, emptyResponse)
      }
      case "addNode" => {
        // TODO check if node name already used
        val ringName = jgetString(request, "ring")
        val nodeName = jgetString(request, "node")
        val host = jgetString(request, "host")
        val port = jgetInt(request, "port")
        val serverName = host + ":" + port
        state.config = state.config.addNode(ringName, nodeName, host, port)
        val f = database ? ("addNode", ringName, nodeName, host, port, state.config)
        Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        log.info("Added node " + ringName + "/" + nodeName)
      }
      case "deleteNode" => {
        val ringName = jgetString(request, "ring")
        val nodeName = jgetString(request, "node")
        state.config = state.config.deleteNode(ringName, nodeName)
        val f = database ? ("deleteNode", ringName, nodeName, state.config)
        Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        log.info("Deleted node " + ringName + "/" + nodeName)
      }
      case "removeEmptyDatabase" => {
        val f = database ? ("isEmpty")
        val (code: String, empty: Boolean) = Await.result(f, 5 seconds)
        if (empty) {
          val stopped = gracefulStop(database, 5 seconds)(system)
          Await.result(stopped, 5 seconds)
          states.delete(databaseName)
          log.info("Database deleted " + databaseName)
        }
        sender ! (Codes.Ok, emptyResponse)
      }
      case "addRing" => {
        val ringName = jgetString(request, "ring")
        val nodes = jgetArray(request, "nodes")
        state.config = state.config.addRing(ringName, nodes)
        val f = database ? ("addRing", ringName, nodes, state.config)
        Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        log.info("Added ring " + ringName)
      }
      case "copyRing" => {
        val ringName = jgetString(request, "ring")
        val fromRingName = jgetString(request, "from")
        val f = database ? ("copyRing", ringName, fromRingName)
        Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
      }
      case "ringReady" => {
        val ringName = jgetString(request, "ring")
        val fromRingName = jgetString(request, "from")
        val f = database ? ("ringReady", ringName, fromRingName)
        val code = Await.result(f, 5 seconds)
        sender ! (code, emptyResponse)
      }
      case "deleteRing1" => {
        val ringName = jgetString(request, "ring")
        state.config = state.config.deleteRing(ringName)
        val f = database ? ("deleteRing1", ringName, state.config)
        Await.result(f, 5 seconds)
        // stop user on ring
        // pass config down
        sender ! (Codes.Ok, emptyResponse)
      }
      case "deleteRing2" => {
        val ringName = jgetString(request, "ring")
        // remove node and ring
        // remove from send
        val f = database ? ("deleteRing2", ringName)
        Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
      }
      case "getLowHigh" => {
        val ringName = jgetString(request, "ring")
        val nodeName = jgetString(request, "node")
        val tableName = jgetString(request, "table")
        val f = database ? ("getLowHigh", ringName, nodeName, tableName)
        val (code: String, result: Json) = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, Compact(result))
      }
      case "setLowHigh" => {
        val ringName = jgetString(request, "ring")
        val nodeName = jgetString(request, "node")
        val tableName = jgetString(request, "table")
        val low = jgetString(request, "low")
        val high = jgetString(request, "high")
        val f = database ? ("setLowHigh", ringName, nodeName, tableName, low, high)
        Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
      }
      case "stopDatabase1" => {
        state.state = DBState.STOPPING
        val f = database ? ("stop", true, true, "", "")
        val v = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
      }
      case "stopDatabase2" => {
        state.state = DBState.STOP
        val f = database ? ("stop2")
        val v = Await.result(f, 5 seconds)
        val stopped = gracefulStop(database, 5 seconds)(system)
        Await.result(stopped, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        state.ref = null
        log.info("Database stopped " + databaseName)
      }
      case "newDatabase" => {
        if (state.state != "") {
          val response = JsonObject("database" -> databaseName)
          sender ! (Codes.ExistDatabase, Compact(response))
        } else {
          val jconfig = jget(request, "config")
          var config = DatabaseConfig(databaseName, jconfig)
          val database = system.actorOf(Props(new ServerDatabase(config, serverConfig, true)), name = databaseName)
          state.ref = database
          state.state = DBState.STARTING
          state.config = config
          val f = database ? ("init")
          val x = Await.result(f, 5 seconds)
          sender ! (Codes.Ok, emptyResponse)
          log.info("Created database " + databaseName)
        }
      }
      case "startDatabase1" => {
        state.state = DBState.STARTING
        val database = system.actorOf(Props(new ServerDatabase(state.config, serverConfig, false)), name = databaseName)
        val f = database ? ("init")
        Await.result(f, 5 seconds)
        state.ref = database
        sender ! (Codes.Ok, emptyResponse)
        log.info("Database starting " + databaseName)
      }
      case "deleteDatabase" => {
        val path = jgetString(serverConfig, "path")
        val fname = path + "/" + databaseName
        val f = new File(fname)
        deletePath(f)
        state.state = ""
        sender ! (Codes.Ok, emptyResponse)
        log.info("Database deleted " + databaseName)
      }
      case "addTable1" => {
        val tableName = jgetString(request, "table")
        state.config = state.config.addTable(tableName)
        val f = database ? ("addTable1", tableName, state.config)
        val v = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        log.info("Added table " + tableName)
      }
      case "addTable2" => {
        val tableName = jgetString(request, "table")
        val f = database ? ("addTable2", tableName)
        val v = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
      }
      case "deleteTable1" => {
        val tableName = jgetString(request, "table")
        state.config = state.config.deleteTable(tableName)
        val f = database ? ("deleteTable1", tableName, state.config)
        val v = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        log.info("Deleted table " + tableName)
      }
      case "deleteTable2" => {
        val tableName = jgetString(request, "table")
        val f = database ? ("deleteTable2", tableName)
        val v = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
      }
    }
  }

  private def stateInfo(state: DatabaseState): Json = {
    val dstate = JsonObject("state" -> state.state)
    val dstate1 = if (state.lock != "") dstate + ("lock" -> state.lock, "lockt" -> state.lockt) else dstate
    if (state.phase != "") dstate1 + ("phase" -> state.phase) else dstate1
  }

  def recInfo(cmd: String, databaseName: String, state: DatabaseState, request: Json) = {
    val database = state.ref
    cmd match {
      case "databaseInfo" => {
        val get = jgetString(request, "get")
        var result = emptyJsonObject
        if (get.contains("s")) {
          result += ("s" -> state.state)
        }
        if (get.contains("c") && state.config != null) {
          result += ("c" -> state.config.toJson)
        }
        sender ! (Codes.Ok, Compact(result))
      }
      case "tableInfo" => {
        val tableName = jgetString(request, "table")
        val options = jget(request, "o")
        val get = jgetString(options, "get")
        val config = state.config
        val table = config.tables(tableName)
        var result = emptyJsonObject
        if (get.contains("r")) {
          if (table.toMap.size > 0 || table.toReduce.size > 0) {
            result += ("r" -> true) // readOnly
          } else {
            result += ("r" -> false)
          }
        }
        if (get.contains("f")) {
          if (table.fromMap.size > 0 || table.fromReduce.size > 0) {
            var f = emptyJsonObject
            if (table.fromMap.size > 0) {
              var a = emptyJsonArray
              for ((to, map) <- table.fromMap) {
                val map1 = jgetObject(map) + ("to" -> to)
                a = map1 +: a
              }
              f += ("map" -> a.reverse)
            }
            if (table.fromReduce.size > 0) {
              var a = emptyJsonArray
              for ((to, red) <- table.fromReduce) {
                val red1 = jgetObject(red) + ("to" -> to)
                a = red1 +: a
              }
              f += ("reduce" -> a)
            }
            result += ("f" -> f)
          }
        }
        if (get.contains("t")) {
          if (table.toMap.size > 0 || table.toReduce.size > 0) {
            var t = emptyJsonObject
            if (table.toMap.size > 0) {
              var a = emptyJsonArray
              for ((from, map) <- table.toMap) {
                val map1 = jgetObject(map) + ("from" -> from)
                a = map1 +: a
              }
              t += ("map" -> a.reverse)
            }
            if (table.toReduce.size > 0) {
              var a = emptyJsonArray
              for ((from, red) <- table.toReduce) {
                val red1 = jgetObject(red) + ("from" -> from)
                a = red1 +: a
              }
              t += ("reduce" -> a)
            }
            if (table.hasPrefix) {
              t += ("prefix" -> table.prefix)
            }
            result += ("t" -> t)
          }
        }
        sender ! (Codes.Ok, Compact(result))
      }
      case "ringInfo" => {
        val ringName = jgetString(request, "ring")
        val options = jget(request, "o")
        val get = jgetString(options, "get")
        var result = emptyJsonObject
        if (get.contains("s")) {
          result += ("s" -> "ok")
        }
        sender ! (Codes.Ok, Compact(result))
      }
      case "nodeInfo" => {
        val ringName = jgetString(request, "ring")
        val nodeName = jgetString(request, "node")
        val options = jget(request, "o")
        val get = jgetString(options, "get")
        val config = state.config
        var result = emptyJsonObject
        if (get.contains("h")) {
          result += ("h" -> config.rings(ringName).nodes(nodeName).server.host)
        }
        if (get.contains("p")) {
          result += ("p" -> config.rings(ringName).nodes(nodeName).server.port)
        }
        if (get.contains("b")) {
          result += ("b" -> config.rings(ringName).prevNodeName(nodeName))
        }
        if (get.contains("f")) {
          result += ("f" -> config.rings(ringName).nextNodeName(nodeName))
        }
        sender ! (Codes.Ok, Compact(result))
      }
      case "allTables" => {
        var result = JsonArray()
        for (name <- state.config.tables.keys) {
          result = name +: result
        }
        sender ! (Codes.Ok, Compact(result.reverse))
      }
      case "allServers" => {
        var result = JsonArray()
        for (name <- state.config.servers.keys) {
          result = name +: result
        }
        sender ! (Codes.Ok, Compact(result.reverse))
      }
      case "allRings" => {
        var result = JsonArray()
        for (name <- state.config.rings.keys) {
          result = name +: result
        }
        sender ! (Codes.Ok, Compact(result.reverse))
      }
      case "allNodes" => {
        val ringName = jgetString(request, "ring")
        state.config.rings.get(ringName) match {
          case Some(info) => {
            //var result = JsonArray()
            //for (name <- info.nodes.keys) {
            //result = name +: result
            //}
            sender ! (Codes.Ok, Compact(info.nodeSeq))
          }
          case None => {
            sender ! (Codes.NoRing, Compact(JsonObject("database" -> databaseName, "ring" -> ringName)))
          }
        }
      }
      case x => {
        log.error("Bad server command: " + x)
        sender ! (Codes.InternalError, Compact(JsonObject("msg" -> "bad server command", "command" -> x)))
      }
    }
  }

  def rec = {
    case d: DeadLetter => {
      var handled = false
      val path = d.recipient.path.toString
      val msg = d.message
      val sender1 = d.sender
      val parts1 = path.split("//")
      if (parts1.size == 2) {
        // Form  ostore/user/database/ring/node/table
        val parts2 = parts1(1).split("/")
        if (parts2.size == 6) {
          val databaseName = parts2(2)
          val ringName = parts2(3)
          val nodeName = parts2(4)
          val tableName = parts2(5)
          msg match {
            case (cmd: String, uid: Long, key: String, value: Any) => {
              handled = true
              states.get(databaseName) match {
                case Some(state) => {
                  val config = state.config
                  config.rings.get(ringName) match {
                    case Some(ringConfig) => {
                      ringConfig.nodes.get(nodeName) match {
                        case Some(nodeConfig) => {
                          config.tables.get(tableName) match {
                            case Some(tableConfig) => {
                              if (state.state != DBState.ACTIVE) {
                                sender1 ! (Codes.NotAvailable, uid, Compact(JsonObject("database" -> databaseName, "state" -> state.state)))
                              } else {
                                // Should never get here
                                log.error("*****Internal Error DeadLetter:" + d.recipient.path + ":" + d.message)
                                sender1 ! (Codes.InternalError, uid, Compact(JsonObject("server" -> serverName, "kind" -> "deadletter", "path" -> path)))
                              }
                            }
                            case None => {
                              sender1 ! (Codes.NoTable, uid, Compact(JsonObject("database" -> databaseName, "ring" -> ringName, "node" -> nodeName, "table" -> tableName)))
                            }
                          }
                        }
                        case None => {
                          sender1 ! (Codes.NoNode, uid, Compact(JsonObject("database" -> databaseName, "ring" -> ringName, "node" -> nodeName)))
                        }
                      }
                    }
                    case None => {
                      sender1 ! (Codes.NoRing, uid, Compact(JsonObject("database" -> databaseName, "ring" -> ringName)))
                    }
                  }
                }
                case None => {
                  sender1 ! (Codes.NoDatabase, uid, Compact(JsonObject("database" -> databaseName)))
                }
              }
            }
            case x =>
          }
        }

      }
      if (!handled) log.error("*****DeadLetter:" + d.sender.path + "=>" + d.recipient.path + ":" + d.message + ":" + serverName)
    }
    case ("lock", guid: String, databaseName: String, rs: String) => {
      val request = Json(rs)
      states.get(databaseName) match {
        case Some(state) => {
          if (state.lock == "" || state.lock == guid) {
            state.lock = guid
            sender ! doLock(databaseName, request)
          } else {
            sender ! (Codes.Lock, emptyResponse)
          }
        }
        case None => {
          val state = states.add(databaseName, guid)
          sender ! doLock(databaseName, request)
        }
      }
    }
    case ("unlock", guid: String, databaseName: String, rs: String) => {
      val request = Json(rs)
      states.get(databaseName) match {
        case Some(state) => {
          if (state.lock == guid) {
            if (state.state == "") {
              states.delete(databaseName)
            }
            state.lock = ""
          }
        }
        case None =>
      }
      sender ! (Codes.Ok, emptyResponse)
    }
    case ("databaseExists", databaseName: String, rs: String) => {
      states.get(databaseName) match {
        case Some(state) => {
          if (state.state == "") {
            sender ! (Codes.NoDatabase, emptyResponse)
          } else {
            sender ! (Codes.Ok, emptyResponse)
          }
        }
        case None => {
          sender ! (Codes.NoDatabase, emptyResponse)
        }
      }
    }
    case ("allDatabases", dummy: String, rs: String) => {
      var result = JsonArray()
      for ((name, state) <- states.all) {
        if (state.state != "") result = name +: result
      }
      sender ! (Codes.Ok, Compact(result.reverse))
    }
    case (cmd: String, guid: String, databaseName: String, rs: String) => {
      states.get(databaseName) match {
        case Some(state) => {
          if (state.lock != guid) {
            sender ! (Codes.Lock, emptyResponse)
          } else {
            val request = Json(rs)
            recAdmin(cmd, databaseName, state, request)
          }
        }
        case None => {
          sender ! (Codes.NoDatabase, Compact(JsonObject("database" -> databaseName)))
        }
      }
    }
    case (cmd: String, databaseName: String, rs: String) => {
      states.get(databaseName) match {
        case Some(state) => {
          if (state.state == "") {
            sender ! (Codes.NoDatabase, Compact(JsonObject("database" -> databaseName)))
          } else {
            val request = Json(rs)
            recInfo(cmd, databaseName, state, request)
          }
        }
        case None => {
          sender ! (Codes.NoDatabase, Compact(JsonObject("database" -> databaseName)))
        }
      }
    }
    case ("serverInfo", rs: String) => {
      val request = Json(rs)
      val serverName = jgetString(request, "server")
      val options = jget(request, "o")
      val get = jgetString(options, "get")
      val requestDatabaseName = jgetString(options, "database")
      var result = emptyJsonObject
      if (get.contains("h")) {
        //val host = state.config.servers(serverName).host
        result += ("h" -> host)
      }
      if (get.contains("p")) {
        //val port = state.config.servers(serverName).port
        result += ("p" -> port)
      }
      if (get.contains("d")) {
        var dinfo = JsonObject()
        for ((databaseName,state) <- states.all) {
          if (requestDatabaseName == "" || requestDatabaseName == databaseName) {
            dinfo += (databaseName -> stateInfo(state))
          }
        }
        result += ("d" -> dinfo)
      }
      sender ! (Codes.Ok, Compact(result))
    }
    case ("start") => {
      log.info("Starting server")
      sender ! Codes.Ok
    }
    case ("stop") => {
      log.info("Stopping server")
      if (restClient != null) restClient.stop()
      states.close()
      store.close()
      sender ! Codes.Ok
    }
  }
}

/**
 * An OStore Server.
 *
 * @param config the server configuration (see Wiki).
 * @param create if true any existing database files are discarded.
 */
class Server(config: Json, create: Boolean) {

  /**
   * Constructor with config an empty configuration and create false.
   *
   * @param config the server configuration (see Wiki).
   */
  def this() = this(emptyJsonObject, false)
  /**
   * Constructor with create false.
   *
   * @param config the server configuration (see Wiki).
   */
  def this(config: Json) = this(config, false)
  /**
   * Constructor with config an empty configuration
   *
   * @param config the server configuration (see Wiki).
   */
  def this(create: Boolean) = this(emptyJsonObject, create)

  private implicit val timeout = Timeout(200 seconds)
  private var serverActor: ActorRef = null
  private var system: ActorSystem = null

  private var akkaConfig = ConfigFactory.load.getConfig("server")
  private val host = jgetString(config, "host")
  private val port = jgetInt(config, "port")
  if (host != "") {
    akkaConfig = akkaConfig.withValue("akka.remote.netty.hostname", ConfigValueFactory.fromAnyRef(host))
  }
  if (port != 0) {
    akkaConfig = akkaConfig.withValue("akka.remote.netty.port", ConfigValueFactory.fromAnyRef(port))
  }
  system = ActorSystem("ostore", akkaConfig)
  serverActor = system.actorOf(Props(new ServerActor(config, create)), name = "@server")
  private val f = serverActor ? ("start")
  Await.result(f, 200 seconds)

  /**
   * Stops the server process.
   * All databases on that server should be stopped.
   */
  def stop() {
    // TODO make sure server is started and no active databases
    val f = serverActor ? ("stop")
    Await.result(f, 5 seconds)
    val f1 = gracefulStop(serverActor, 5 seconds)(system) // will stop all its children too!
    Await.result(f1, 5 seconds)
    system.shutdown()
    // TODO throw exception if any internal errors detected
  }
}

/**
 * An object for running on OStore from SBT or the command line.
 */
object Server {
  /**
   * Main routine.
   *
   * @param args The first arg is the path to the server configuration file (see wiki). If
   * absent the path defaults to '''config/server.json'''.
   */
  def main(args: Array[String]) {
    val fname = if (args.size > 0) { args(0) } else { "config/server.json" }
    val config = Source.fromFile(fname).mkString
    val server = new Server(Json(config))
  }
}

