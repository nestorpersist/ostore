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
    case x => println("*****Other Event:" + x)
  }
}

private[persist] class DatabaseInfo(
  var dbRef: ActorRef,
  var config: DatabaseConfig,
  var state: String)

private[persist] class Server(serverConfig: Json, create: Boolean) extends CheckedActor {
  private implicit val timeout = Timeout(5 seconds)

  private def deletePath(f: File) {
    if (f.isDirectory()) {
      for (f1: File <- f.listFiles()) {
        deletePath(f1)
      }
    }
    f.delete()
  }

  private val host = jgetString(serverConfig, "host")
  private val port = jgetInt(serverConfig, "port")
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

  private val restPort = jgetInt(serverConfig, "rest")
  if (restPort != 0) {
    RestClient1.system = system
    Http.start(system, restPort)
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
    databases.get(databaseName) match {
      case Some(info) => {
        val result = if (getInfo) {
          var servers = JsonArray()
          for (name <- info.config.servers.keys) {
            servers = name +: servers
          }
          var result = JsonObject(("s" -> info.state), ("servers" -> servers.reverse))
          if (tableName != "") {
            info.config.tables.get(tableName) match {
              case Some(tinfo) => {}
              case None => {
                result += ("tableAbsent" -> true)
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
        var result = JsonObject(("s" -> "none"))
        (Codes.Ok, Compact(result))
      }
    }
  }

  def rec = {
    case d: DeadLetter => {
      var handled = false
      val path = d.recipient.path.toString
      val msg = d.message
      val sender1 = d.sender
      val parts1= path.split("//")
      if (parts1.size == 2) {
        // Form  ostore/user/database/ring/node/table
        val parts2 = parts1(1).split("/")
        if (parts2.size == 6) {
          val databaseName = parts2(2)
          val ringName = parts2(3)
          val nodeName = parts2(4)
          val tableName = parts2(5)
          msg match {
            case (cmd:String,uid:Long, key:String, value:Any) => {
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
                                // Should never get here
                                println("*****Internal Error DeadLetter:" + d.recipient.path + ":" + d.message)
                                sender1 ! (Codes.InternalError, uid, path)
                              }
                              case None => {
                                sender1 ! (Codes.NoTable, uid, tableName)
                              }
                            }
                          }
                          case None =>{
                            sender1 ! (Codes.NoNode, uid, nodeName)
                          }
                        }
                      }
                      case None => {
                        sender1 ! (Codes.NoRing, uid, ringName)
                      }
                    }
                  }
                  case None => {
                    sender1 ! (Codes.NoDatabaase, uid, databaseName)
                  }
                }
            }
            case x=> 
          }
        }
        
      }
      if (! handled) println("*****DeadLetter:" + d.recipient.path + ":" + d.message)
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
        sender ! (Codes.Locked, "")
      }
    }
    case ("unlock", databaseName: String, rs: String) => {
      val request = Json(rs)
      val guid = jgetString(request, "guid")
      val lock = locks.getOrElse(databaseName, "")
      if (lock == "") {
        sender ! (Codes.Ok, "")
      } else if (lock == guid) {
        locks -= databaseName
        sender ! (Codes.Ok, "")
      } else {
        sender ! (Codes.Locked, "")
      }
    }
    case ("stopBalance", databaseName: String, rs: String) => {
      val request = Json(rs)
      val ringName = jgetString(request, "ring")
      val nodeName = jgetString(request, "node")
      databases.get(databaseName) match {
        case Some(info) => {
          val database = info.dbRef
          val f = database ? ("stopBalance", ringName, nodeName)
          Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("startBalance", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val database = info.dbRef
          val f = database ? ("startBalance")
          Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("busyBalance", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val database = info.dbRef
          val f = database ? ("busyBalance")
          val (code:String, result:String) = Await.result(f, 5 seconds)
          sender ! (code, "")
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("addNode", databaseName: String, rs: String) => {
      val request = Json(rs)
      val ringName = jgetString(request, "ring")
      val nodeName = jgetString(request, "node")
      val host = jgetString(request, "host")
      val port = jgetInt(request, "port")
      val serverName = host + ":" + port
      databases.get(databaseName) match {
        case Some(info) => {
          // TODO check node name not already used
          info.config = info.config.addNode(ringName, nodeName, serverName)
          storeTable.putMeta(databaseName, Compact(info.config.toJson))
          val database = info.dbRef
          val f = database ? ("addNode", ringName, nodeName, info.config)
          Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
          println("Added node " + ringName +"/" + nodeName)
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("deleteNode", databaseName:String , rs:String) =>{
      val request = Json(rs)
      val ringName = jgetString(request, "ring")
      val nodeName = jgetString(request, "node")
      databases.get(databaseName) match {
        case Some(info) => {
          info.config = info.config.deleteNode(ringName, nodeName)
          storeTable.putMeta(databaseName, Compact(info.config.toJson))
          val database = info.dbRef
          val f = database ? ("deleteNode", ringName, nodeName, info.config)
          val (code:String,empty:Boolean) = Await.result(f, 5 seconds)
          if (empty) {
              val stopped = gracefulStop(database, 5 seconds)(system)
              Await.result(stopped, 5 seconds)
              databases -= databaseName
              println("Database deleted " + databaseName)
          }
          sender ! (Codes.Ok, "")
          println("Delete node " + ringName +"/" + nodeName)
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }      
    }
    case ("getLowHigh", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val request = Json(rs)
          val ringName = jgetString(request, "ring")
          val nodeName = jgetString(request, "node")
          val tableName = jgetString(request, "table")
          val database = info.dbRef
          val f = database ? ("getLowHigh", ringName, nodeName, tableName)
          val (code: String, result: Json) = Await.result(f, 5 seconds)
          sender ! (Codes.Ok, Compact(result))
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("setLowHigh", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val request = Json(rs)
          val ringName = jgetString(request, "ring")
          val nodeName = jgetString(request, "node")
          val tableName = jgetString(request, "table")
          val low = jgetString(request,"low")
          val high = jgetString(request, "high")
          val database = info.dbRef
          val f = database ? ("setLowHigh", ringName, nodeName, tableName, low, high)
          Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("databaseinfo", databaseName: String, os: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val options = Json(os)
          val infos = jgetString(options, "info")
          var result = emptyJsonObject
          if (infos.contains("s")) {
            result += ("s" -> info.state)
          }
          if (infos.contains("c")) {
            result += ("c" -> info.config.toJson)
          }
          sender ! (Codes.Ok, Compact(result))
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("tableinfo", databaseName: String, tableName: String, os: String) => {
      val options = Json(os)
      sender ! (Codes.Ok, Compact(emptyJsonObject))
    }
    case ("ringinfo", databaseName: String, ringName: String, os: String) => {
      val options = Json(os)
      sender ! (Codes.Ok, Compact(emptyJsonObject))
    }
    case ("serverinfo", databaseName: String, serverNAme: String, os: String) => {
      val options = Json(os)
      sender ! (Codes.Ok, Compact(emptyJsonObject))
    }
    case ("nodeinfo", databaseName: String, ringName: String, nodeName: String, os: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val config = info.config
          val options = Json(os)
          val infos = jgetString(options, "info")
          var result = emptyJsonObject
          if (infos.contains("h")) {
            result += ("h" -> config.rings(ringName).nodes(nodeName).server.host)
          }
          if (infos.contains("p")) {
            result += ("p" -> config.rings(ringName).nodes(nodeName).server.port)
          }
          sender ! (Codes.Ok, Compact(result))
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)

        }
      }
    }
    case ("newDatabase", databaseName: String, rs: String) => {
      if (databases.contains(databaseName)) {
        sender ! ("AlreadyPresent", "")
      } else {
        val request = Json(rs)
        val dbConfig = jget(request, "config")
        val dbConf = Compact(dbConfig)
        var config = DatabaseConfig(databaseName, dbConfig)
        storeTable.putMeta(databaseName, dbConf)
        val database = system.actorOf(Props(new ServerDatabase(config, serverConfig, true)), name = databaseName)
        val info = new DatabaseInfo(database, config, "starting")
        databases += (databaseName -> info)
        val f = database ? ("start1")
        val x = Await.result(f, 5 seconds)
        sender ! (Codes.Ok, "")
        println("Created Database " + databaseName)
      }
    }
    case ("stopDatabase1", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          info.state = "stopping"
          val database = info.dbRef
          val f = database ? ("stop1")
          val v = Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }
    case ("stopDatabase2", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          info.state = "stop"
          val database = info.dbRef
          val f = database ? ("stop2")
          val v = Await.result(f, 5 seconds)
          val stopped = gracefulStop(database, 5 seconds)(system)
          Await.result(stopped, 5 seconds)
          sender ! (Codes.Ok, "")
          info.dbRef = null
          println("Database stopped " + databaseName)
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }
    case ("startDatabase1", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val database = system.actorOf(Props(new ServerDatabase(info.config, serverConfig, false)), name = databaseName)
          val f = database ? ("start1")
          Await.result(f, 5 seconds)
          info.state = "starting"
          info.dbRef = database
          sender ! (Codes.Ok, "")
          println("Database starting " + databaseName)
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }
    case ("startDatabase2", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          val database = info.dbRef
          val f = database ? ("start2")
          Await.result(f, 5 seconds)
          info.state = "active"
          sender ! (Codes.Ok, "")
          println("Database started " + databaseName)
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }
    case ("deleteDatabase", databaseName: String, rs: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          println("Deleting database " + databaseName)
          val path = jgetString(serverConfig, "path")
          val fname = path + "/" + databaseName
          val f = new File(fname)
          deletePath(f)
          databases -= databaseName
          sender ! (Codes.Ok, "")
          storeTable.remove(databaseName)
          println("Database deleted " + databaseName)
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }
    case ("allDatabases") => {
      var result = JsonArray()
      for ((name, info) <- databases) {
        //val o = JsonObject("name" -> name, "state" -> info.state)
        result = name +: result
      }
      sender ! (Codes.Ok, Compact(result.reverse))
    }
    case ("alltables", databaseName: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          var result = JsonArray()
          for (name <- info.config.tables.keys) {
            result = name +: result
          }
          sender ! (Codes.Ok, Compact(result.reverse))
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("allservers", databaseName: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          var result = JsonArray()
          for (name <- info.config.servers.keys) {
            result = name +: result
          }
          sender ! (Codes.Ok, Compact(result.reverse))
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("allrings", databaseName: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          var result = JsonArray()
          for (name <- info.config.rings.keys) {
            result = name +: result
          }
          sender ! (Codes.Ok, Compact(result.reverse))
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("allnodes", databaseName: String, ringName: String) => {
      databases.get(databaseName) match {
        case Some(info) => {
          info.config.rings.get(ringName) match {
            case Some(info) => {
              var result = JsonArray()
              for (name <- info.nodes.keys) {
                result = name +: result
              }
              sender ! (Codes.Ok, Compact(result.reverse))
            }
            case None => {
              sender ! (Codes.Exist, "ring:" + databaseName + "/" + ringName)
            }
          }
        }
        case None => {
          sender ! (Codes.Exist, "database:" + databaseName)
        }
      }
    }
    case ("addTable1", databaseName: String, rs: String) => {
      val request = Json(rs)
      val tableName = jgetString(request, "table")
      databases.get(databaseName) match {
        case Some(info) => {
          info.config = info.config.addTable(tableName)
          storeTable.putMeta(databaseName, Compact(info.config.toJson))
          val database = info.dbRef
          val f = database ? ("addTable1", tableName, info.config)
          val v = Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
          println("Added table " + tableName)
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }
    case ("addTable2", databaseName: String, rs: String) => {
      val request = Json(rs)
      val tableName = jgetString(request, "table")
      databases.get(databaseName) match {
        case Some(info) => {
          val database = info.dbRef
          val f = database ? ("addTable2", tableName)
          val v = Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }
    case ("deleteTable1", databaseName: String, rs: String) => {
      val request = Json(rs)
      val tableName = jgetString(request, "table")
      databases.get(databaseName) match {
        case Some(info) => {
          val database = info.dbRef
          info.config = info.config.deleteTable(tableName)
          storeTable.putMeta(databaseName, Compact(info.config.toJson))
          val f = database ? ("deleteTable1", tableName, info.config)
          val v = Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
          println("Deleted table " + tableName)
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }
    case ("deleteTable2", databaseName: String, rs: String) => {
      val request = Json(rs)
      val tableName = jgetString(request, "table")
      databases.get(databaseName) match {
        case Some(info) => {
          val database = info.dbRef
          val f = database ? ("deleteTable2", tableName)
          val v = Await.result(f, 5 seconds)
          sender ! (Codes.Ok, "")
        }
        case None => {
          sender ! (Codes.Exist, "")
        }
      }
    }

    case ("start") => {
      log.info("Starting server.")
      sender ! Codes.Ok
    }
    case ("stop") => {
      log.info("Stopping server.")
      if (restPort != 0) Http.stop
      storeTable.close()
      sender ! Codes.Ok
    }
    case x: String => println("databaseFail:" + x)
  }
}

object Server {
  private implicit val timeout = Timeout(200 seconds)
  private var server: ActorRef = null
  private var system: ActorSystem = null

  def start(config: Json, create: Boolean = false) {
    // TODO get port from config
    system = ActorSystem("ostore", ConfigFactory.load.getConfig("server"))
    server = system.actorOf(Props(new Server(config, create)), name = "@server")
    val f = server ? ("start")
    Await.result(f, 200 seconds)
  }

  def stop {
    // TODO make sure server is started and no active databases
    val f = server ? ("stop")
    Await.result(f, 5 seconds)
    val f1 = gracefulStop(server, 5 seconds)(system) // will stop all its children too!
    Await.result(f1, 5 seconds)
    system.shutdown()
    // TODO throw exception if any internal errors detected
  }

  def main(args: Array[String]) {
    val fname = if (args.size > 0) { args(0) } else { "config/server.json" }
    println("Server Starting")
    val config = Source.fromFile(fname).mkString
    start(Json(config))
  }
}

