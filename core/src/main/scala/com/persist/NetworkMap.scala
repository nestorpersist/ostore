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

import scala.collection.JavaConversions._
import akka.actor.Actor
import Actor._
import akka.actor.ActorRef
import JsonOps._
import akka.util.Timeout
import akka.pattern._
import akka.util.duration._
import akka.dispatch._
import akka.actor.ActorSystem
import akka.actor.Props
import scala.collection.immutable.TreeMap

// Actor Paths
//       /user/@srv
//       /user/database
//       /user/database/@send
//       /user/database/node
//       /user/database/node/@mon
//       /user/database/node/table

class Range {
  private var low: String = ""
  private var high: String = ""
  def get() = { (low, high) }
  def put(low: String, high: String) {
    this.low = low
    this.high = high
  }
}

class NetworkMap(system: ActorSystem, val databaseName: String, config: Json) {

  // TODO temp constructor as Json rep is changed!
  //def this(system:ActorSystem,databaseName:String,config:String) = this(system,databaseName,Json(config))

  class ServerInfo(val name: String, val host: String, val port: Int) {
    lazy val ref = system.actorFor("akka://ostore@" + host + ":" + port + "/user/@svr")
    lazy val dbRef = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" + databaseName)
    lazy val sendRef = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" + databaseName + "/@send")
  }

  class NodeTableInfo(nodeName: String, val name: String, host: String, port: Int, low: String, high: String) {
    val range = new Range()
    range.put(low, high)
    lazy val ref: ActorRef = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" + databaseName + "/" + nodeName + "/" + name)
  }

  class TableInfo(tableName: String) {
    var prefix:JsonArray = JsonArray()
    var toMap = Map[String, Json]()
    var fromMap = Map[String, Json]()
    var toReduce = Map[String, Json]()
    var fromReduce:Json = null
  }

  class NodeInfo(val ringName: String, val name: String, val pos: Int, host: String, port: Int, low: String, high: String) {
    var tables = Map[String, NodeTableInfo]()
    for (tableName <- allTables) {
      val tableInfo = new NodeTableInfo(name, tableName, host, port, low, high)
      tables = tables + (tableName -> tableInfo)
    }
    lazy val ref: ActorRef = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" + databaseName + "/" + name)
    lazy val monitorRef: ActorRef = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" + databaseName + "/" + name + "/@mon")

    private val serverName = host + ":" + port
    val server = if (servers.contains(serverName)) {
      servers(serverName)
    } else {
      val server = new ServerInfo(serverName, host, port)
      servers = servers + (serverName -> server)
      server
    }
  }

  class RingInfo(val name: String) {
    // serverName -> ServerInfo
    var map = Map[String, NodeInfo]()
    // server names
    var seq = List[String]()
  }

  // host:port -> ServerInfo
  private var servers = Map[String, ServerInfo]()

  // ringName -> RingInfo
  private var rings = Map[String, RingInfo]()

  private var tables = new TreeMap[String, TableInfo]()
  //private var tables = Map[String, TableInfo]()

  // srcTableName dstTableName -> MapInfo
  //private var maps = Map[String, Map[String, Json]]()

  private def nodeInit(node: Json, ringInfo: RingInfo, pos: Int, defaultHost: String, defaultPort: Int, low: String, high: String) {
    val name = jgetString(node, "name")
    val host = jget(node, "host") match {
      case null => defaultHost
      case n: Json => jgetString(n)
    }
    val port = jget(node, "port") match {
      case null => defaultPort
      case n: Int => n
    }
    val nodeInfo = new NodeInfo(ringInfo.name, name, pos, host, port, low, high)
    ringInfo.map = ringInfo.map + (name -> nodeInfo)
    ringInfo.seq = ringInfo.seq :+ name
  }

  private def initMap(to:String, map:Json) {
      val from = jgetString(map, "from")
      tables(to).fromMap += (from -> map)
      tables(from).toMap += (to -> map)
  }

  private def init(config: Json) {
    rings = Map[String, RingInfo]()
    val tabs = jgetArray(config, "tables")
    for (tab <- tabs) {
      val name = jgetString(tab, "name")
      val info = new TableInfo(name)
      tables = tables + (name -> info)
    }
    for (tab <- tabs) {
      val to = jgetString(tab, "name")
      val prefix = jget(tab,"prefix") match {
        case a:JsonArray => a
        case obj:JsonObject => JsonArray(obj)
        case x => JsonArray()
      }
      tables(to).prefix = prefix
      val map = jget(tab, "map")
      map match {
        case a:JsonArray => {
          for (m<-a) {
            initMap(to,m)
          }
        }
        case m:JsonObject => initMap(to,m)
        case x =>
      }
      val reduce = jget(tab, "reduce")
      if (reduce != null) {
        val from = jgetString(reduce, "from")
        tables(to).fromReduce = reduce
        tables(from).toReduce += (to -> reduce)
        //val iname = "@" + to // intermediate reduce table
        //val info = new TableInfo(iname)
        //info.interReduce = reduce
        //tables = tables + (iname -> info)
      }
    }
    val defaults = jget(config, "defaults")
    val defaultHost = jgetString(defaults, "host")
    val defaultPort = jgetInt(defaults, "port")
    for (ring <- jgetArray(config, "rings")) {
      val ringName = jgetString(ring, "name")
      var ringMap = Map[String, NodeInfo]()
      val rinfo = new RingInfo(ringName)
      var pos: Int = 0
      val nodes = jgetArray(ring, "nodes")
      for (node <- nodes) {
        val low = pos
        val high = if (pos == nodes.size() - 1) { 0 } else { pos + 1 }
        val lows = keyEncode(low)
        val highs = keyEncode(high)
        nodeInit(node, rinfo, pos, defaultHost, defaultPort, lows, highs)
        pos = pos + 1
      }
      rings = rings + (ringName -> rinfo)
    }
  }

  def allServers = servers.valuesIterator

  def allRings = rings.keysIterator
  
  def allRingsSize = rings.size

  def allNodes(ringName: String) = rings(ringName).map.valuesIterator

  def allTables = tables.keys

  def nodes(ringName: String) = rings(ringName).map.size

  def setRange(ringName: String, name: String, table: String, low: String, high: String) {
    val sInfo = rings(ringName).map(name)
    sInfo.tables(table).range.put(low, high)
  }

  def serverInfo(serverName: String) = servers(serverName)
  def ringInfo(ringName: String) = rings(ringName)
  def nodeInfo(ringInfo: RingInfo, nodeName: String) = ringInfo.map(nodeName)
  def nodeTableInfo(nodeInfo: NodeInfo, tableName: String) = nodeInfo.tables(tableName)
  def tableInfo(tableName: String) = tables(tableName)

  def get(ringName: String, nodeName: String) = rings(ringName).map(nodeName).ref
  def get(ringName: String, nodeName: String, tableName: String) = rings(ringName).map(nodeName).tables(tableName).ref

  def getForKey(ringName: String, tab: String, key: String, less: Boolean): Option[NodeInfo] = {
    // TODO this code should eventually be changed to use a tree
    // rather than linear search (N => LOG N)
    if (!tables.contains(tab)) return None
    val s = rings(ringName).map
    var si: NodeInfo = null
    var prev = ""
    for (nodeInfo <- allNodes(ringName)) {
      if (nodes(ringName) == 1) return Some(nodeInfo)
      val (low: String, high: String) = nodeInfo.tables(tab).range.get()
      if (si == null) {
        // skip the first, but save
        si = nodeInfo
      } else {
        if (prev <= high) {
          if (less) {
            if (prev < key && key <= high) return Some(nodeInfo)
          } else {
            if (prev <= key && key < high) return Some(nodeInfo)
          }
        } else {
          if (less) {
            if (prev < key || key <= high) return Some(nodeInfo)
          } else {
            if (prev <= key || key < high) return Some(nodeInfo)
          }
        }
      }
      prev = high
    }
    return Some(si) // since nothing else worked, it must be the first! (but this fragile, should rework)
  }

  def getNext(ringName: String, name: String): String = {
    val pos = rings(ringName).map(name).pos
    val seq = rings(ringName).seq
    val nextPos = if (pos == seq.length - 1) { 0 } else { pos + 1 }
    seq(nextPos)
  }

  def getPrev(ringName: String, name: String): String = {
    val pos = rings(ringName).map(name).pos
    val seq = rings(ringName).seq
    val nextPos = if (pos == 0) { seq.length - 1 } else { pos - 1 }
    seq(nextPos)
  }

  init(config)
}