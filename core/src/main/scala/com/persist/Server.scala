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

private[persist] class DatabaseInfo(
  var dbRef: ActorRef,
  var config: DatabaseConfig,
  var state: String)

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

  private val path = jgetString(serverConfig, "path")
  private var exists = false
  private val store = path match {
    case "" => new InMemoryStore(context, "@server", "", false)
    case _ =>
      val fname = path + "/" + "@server" + "/" + serverName
      val f = new File(fname)
      exists = f.exists()
      new Store(context, "@server", fname, !exists || create)
  }
  private val system = context.system
  private val sendServer = new SendServer(system)
  private val storeTable = store.getTable(serverName)
  private val listener = system.actorOf(Props[Listener])

  system.eventStream.subscribe(self, classOf[DeadLetter])
  system.eventStream.subscribe(listener, classOf[RemoteLifeCycleEvent])

  private var databases = TreeMap[String, DatabaseInfo]()

  // databaseName -> locking guid or "" if unlocked
  private var locks = TreeMap[String, String]()

  private val restInfo = jget(serverConfig, "rest")
  private var restClient: RestClient = null
  if (restInfo != null) {
    var httpPort = jgetInt(restInfo, "port")
    if (httpPort == 0) httpPort = 8081
    var clientPort = jgetInt(restInfo, "client", "port")
    if (clientPort == 0) clientPort
    if (clientPort == 0) port = akkaClientConfig.getInt("akka.remote.netty.port")
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

  if (exists) {
    var done = false
    var key = storeTable.first()
    do {
      key match {
        case Some(databaseName: String) => {
          val dbConf = storeTable.getMeta(databaseName) match {
            case Some(s: String) => s
            case None => ""
          }
          val dbConfig = Json(dbConf)
          val config = DatabaseConfig(databaseName, dbConfig)
          val info = new DatabaseInfo(null, config, "stop")
          databases += (databaseName -> info)
          key = storeTable.next(databaseName, false)
        }
        case None => done = true
      }
    } while (!done)
  }

  private def doLock(databaseName: String, request: Json): (String, String) = {
    val getInfo = jgetBoolean(request, "getinfo")
    val tableName = jgetString(request, "table")
    val ringName = jgetString(request, "ring")
    val nodeName = jgetString(request, "node")
    databases.get(databaseName) match {
      case Some(info) => {
        val result = if (getInfo) {
          var servers = JsonArray()
          for (name <- info.config.servers.keys) {
            servers = name +: servers
          }
          var result = JsonObject("servers" -> servers.reverse , "s" -> info.state)
          if (tableName != "") {
            info.config.tables.get(tableName) match {
              case Some(tinfo) => 
              case None => {
                result += ("tableAbsent" -> true)
              }
            }
          }
          if (ringName != "") {
            info.config.rings.get(ringName) match {
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
          emptyJsonObject
        }
        (Codes.Ok, Compact(result))
      }
      case None => {
        var result = JsonObject("databaseAbsent" -> true, "s" -> "none")
        (Codes.Ok, Compact(result))
      }
    }
  }

  def rec1(cmd: String, databaseName: String, info: DatabaseInfo, request: Json) = {
    val database = info.dbRef
    cmd match {
      case "start" => {
        val balance = jgetBoolean(request, "balance")
        val user = jgetBoolean(request, "user")
        val ring = jgetString(request, "ring")
        val node = jgetString(request, "node")
        val f = database ? ("start", ring, node, balance, user)
        Await.result(f, 5 seconds)
        if (user) {
          info.state = "active"
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
        val config = info.config.enableRing(ringName, avail)
        // TODO enable user ops on new ring node table nodes
        sender ! (Codes.Ok, emptyResponse)
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
        info.config = info.config.addNode(ringName, nodeName, host, port)
        storeTable.putMeta(databaseName, Compact(info.config.toJson))
        val f = database ? ("addNode", ringName, nodeName, host, port, info.config)
        Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        log.info("Added node " + ringName + "/" + nodeName)
      }
      case "deleteNode" => {
        val ringName = jgetString(request, "ring")
        val nodeName = jgetString(request, "node")
        info.config = info.config.deleteNode(ringName, nodeName)
        storeTable.putMeta(databaseName, Compact(info.config.toJson))
        val f = database ? ("deleteNode", ringName, nodeName, info.config)
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
          databases -= databaseName
          log.info("Database deleted " + databaseName)
        }
        sender ! (Codes.Ok, emptyResponse)
      }
      case "addRing" => {
        val ringName = jgetString(request, "ring")
        val nodes = jgetArray(request, "nodes")
        info.config = info.config.addRing(ringName, nodes)
        storeTable.putMeta(databaseName, Compact(info.config.toJson))
        val f = database ? ("addRing", ringName, nodes, info.config)
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
        info.config = info.config.deleteRing(ringName)
        storeTable.putMeta(databaseName, Compact(info.config.toJson))
        val f = database ? ("deleteRing1", ringName, info.config)
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
      case "databaseInfo" => {
        val get = jgetString(request, "get")
        var result = emptyJsonObject
        if (get.contains("s")) {
          result += ("s" -> info.state)
        }
        if (get.contains("c")) {
          result += ("c" -> info.config.toJson)
        }
        sender ! (Codes.Ok, Compact(result))
      }
      case "stopDatabase1" => {
        info.state = "stopping"
        val f = database ? ("stop", true, true, "", "")
        val v = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
      }
      case "stopDatabase2" => {
        info.state = "stop"
        val f = database ? ("stop2")
        val v = Await.result(f, 5 seconds)
        val stopped = gracefulStop(database, 5 seconds)(system)
        Await.result(stopped, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        info.dbRef = null
        log.info("Database stopped " + databaseName)
      }
      case "startDatabase1" => {
        val database = system.actorOf(Props(new ServerDatabase(info.config, serverConfig, false)), name = databaseName)
        val f = database ? ("init")
        Await.result(f, 5 seconds)
        info.state = "starting"
        info.dbRef = database
        sender ! (Codes.Ok, emptyResponse)
        log.info("Database starting " + databaseName)
      }
      case "deleteDatabase" => {
        val path = jgetString(serverConfig, "path")
        val fname = path + "/" + databaseName
        val f = new File(fname)
        deletePath(f)
        databases -= databaseName
        sender ! (Codes.Ok, emptyResponse)
        storeTable.remove(databaseName)
        log.info("Database deleted " + databaseName)
      }
      case "addTable1" => {
        val tableName = jgetString(request, "table")
        info.config = info.config.addTable(tableName)
        storeTable.putMeta(databaseName, Compact(info.config.toJson))
        val f = database ? ("addTable1", tableName, info.config)
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
        info.config = info.config.deleteTable(tableName)
        storeTable.putMeta(databaseName, Compact(info.config.toJson))
        val f = database ? ("deleteTable1", tableName, info.config)
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
      case "serverInfo" => {
        val serverName = jgetString(request, "server")
        val options = jget(request, "o")
        val get = jgetString(options, "get")
        var result = emptyJsonObject
        if (get.contains("h")) {
          val host = info.config.servers(serverName).host
          result += ("h" -> host)
        }
        if (get.contains("p")) {
          val port = info.config.servers(serverName).port
          result += ("p" -> port)
        }
        sender ! (Codes.Ok, Compact(result))
      }
      case "tableInfo" => {
        val tableName = jgetString(request, "table")
        val options = jget(request, "o")
        val get = jgetString(options, "get")
        val config = info.config
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
        val config = info.config
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
        for (name <- info.config.tables.keys) {
          result = name +: result
        }
        sender ! (Codes.Ok, Compact(result.reverse))
      }
      case "allServers" => {
        var result = JsonArray()
        for (name <- info.config.servers.keys) {
          result = name +: result
        }
        sender ! (Codes.Ok, Compact(result.reverse))
      }
      case "allRings" => {
        var result = JsonArray()
        for (name <- info.config.rings.keys) {
          result = name +: result
        }
        sender ! (Codes.Ok, Compact(result.reverse))
      }
      case "allNodes" => {
        val ringName = jgetString(request, "ring")
        info.config.rings.get(ringName) match {
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
              databases.get(databaseName) match {
                case Some(info) => {
                  val config = info.config
                  config.rings.get(ringName) match {
                    case Some(ringConfig) => {
                      ringConfig.nodes.get(nodeName) match {
                        case Some(nodeConfig) => {
                          config.tables.get(tableName) match {
                            case Some(tableConfig) => {
                              if (info.state != "active") {
                                sender1 ! (Codes.NotAvailable, uid, Compact(JsonObject("database" -> databaseName, "state" -> info.state)))
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
    case ("lock", databaseName: String, rs: String) => {
      val request = Json(rs)
      val guid = jgetString(request, "guid")
      val lock = locks.getOrElse(databaseName, "")
      if (lock == "") {
        locks += (databaseName -> guid)
        sender ! doLock(databaseName, request)
      } else if (lock == guid) {
        sender ! doLock(databaseName, request)
      } else {
        sender ! (Codes.Locked, emptyResponse)
      }
    }
    case ("unlock", databaseName: String, rs: String) => {
      val request = Json(rs)
      val guid = jgetString(request, "guid")
      val lock = locks.getOrElse(databaseName, "")
      if (lock == "") {
        sender ! (Codes.Ok, emptyResponse)
      } else if (lock == guid) {
        locks -= databaseName
        sender ! (Codes.Ok, emptyResponse)
      } else {
        sender ! (Codes.Locked, emptyResponse)
      }
    }
    case ("databaseExists", databaseName: String, rs: String) => {
      if (databases.contains(databaseName)) {
        sender ! (Codes.Ok, emptyResponse)
      } else {
        sender ! (Codes.NoDatabase, emptyResponse)
      }
    }
    case ("newDatabase", databaseName: String, rs: String) => {
      if (databases.contains(databaseName)) {
        val response = JsonObject("database" -> databaseName)
        sender ! (Codes.ExistDatabase, Compact(response))
      } else {
        val request = Json(rs)
        val dbConfig = jget(request, "config")
        val dbConf = Compact(dbConfig)
        var config = DatabaseConfig(databaseName, dbConfig)
        storeTable.putMeta(databaseName, dbConf)
        val database = system.actorOf(Props(new ServerDatabase(config, serverConfig, true)), name = databaseName)
        val info = new DatabaseInfo(database, config, "starting")
        databases += (databaseName -> info)
        val f = database ? ("init")
        val x = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, emptyResponse)
        log.info("Created database " + databaseName)
      }
    }
    case ("allDatabases", dummy: String, rs: String) => {
      var result = JsonArray()
      for ((name, info) <- databases) {
        result = name +: result
      }
      sender ! (Codes.Ok, Compact(result.reverse))
    }
    case (cmd: String, databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val request = Json(rs)
          rec1(cmd, databaseName, info, request)
        }
        case None => {
          sender ! (Codes.NoDatabase, Compact(JsonObject("database" -> databaseName)))
        }
      }
    }
    case ("start") => {
      log.info("Starting server")
      sender ! Codes.Ok
    }
    case ("stop") => {
      log.info("Stopping server")
      if (restClient != null) restClient.stop()
      storeTable.close()
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
   * @param args The first arg is the path to the server configuration file. If
   * absent the path defaults to config/server.json.
   */
  def main(args: Array[String]) {
    val fname = if (args.size > 0) { args(0) } else { "config/server.json" }
    val config = Source.fromFile(fname).mkString
    val server = new Server(Json(config))
  }
}

