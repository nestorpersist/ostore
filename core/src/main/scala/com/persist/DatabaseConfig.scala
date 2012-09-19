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
import scala.collection.immutable.TreeMap
import akka.actor.ActorRef
import akka.actor.ActorSystem

// TODO retain deleted node, table and ring names

private[persist] case class ServerConfig(
  val host: String,
  val port: Int) {
  val name = host + ":" + port
}

private[persist] case class RingConfig(
  val name: String,
  // serverName -> ServerConfig
  val nodes: Map[String, NodeConfig],
  // server names
  val nodeSeq: List[String],
  val available: Boolean = true) {

  def nodePosition(nodeName: String): Int = {
    nodes(nodeName).pos
  }

  def nextNodeName(nodeName: String): String = {
    val pos = nodes(nodeName).pos + 1
    if (pos > nodeSeq.size) {
      println(nodeName + ":" + nodes(nodeName) + ":" + pos)
    }
    val pos1 = if (pos == nodeSeq.size) 0 else pos
    nodeSeq(pos1)
  }

  def prevNodeName(nodeName: String): String = {
    val pos = nodes(nodeName).pos - 1
    val pos1 = if (pos < 0) nodeSeq.size - 1 else pos
    nodeSeq(pos1)
  }

}

private[persist] case class NodeConfig(
  val ringName: String,
  val name: String,
  val server: ServerConfig,
  val pos: Int)

private[persist] case class TableConfig(
  val tableName: String,
  val hasPrefix: Boolean,
  val prefix: JsonArray, 
  val toMap: Map[String, Json],
  val fromMap: Map[String, Json],
  val toReduce: Map[String, Json],
  val fromReduce: Map[String, Json])

private[persist] object DatabaseConfig {

  private def getMap(map: Map[String, Map[String, Json]], name: String): Map[String, Json] = {
    if (!map.contains(name)) { Map[String, Json]() } else { map(name) }
  }

  def apply(databaseName: String, config: Json): DatabaseConfig = {
    val tabs = jgetArray(config, "tables")

    var fromMap = Map[String, Map[String, Json]]()
    var fromReduce = Map[String, Map[String, Json]]()
    var toMap = Map[String, Map[String, Json]]()
    var toReduce = Map[String, Map[String, Json]]()
    for (tab <- tabs) {
      val to = jgetString(tab, "name")
      val map = jget(tab, "map")
      map match {
        case a: JsonArray => {
          for (m <- a) {
            val from = jgetString(m, "from")
            toMap += (to -> (getMap(toMap, to) + (from -> m)))
            fromMap += (from -> (getMap(fromMap, from) + (to -> m)))
          }
        }
        case m: JsonObject => {
          val from = jgetString(m, "from")
          toMap += (to -> (getMap(toMap, to) + (from -> m)))
          fromMap += (from -> (getMap(fromMap, from) + (to -> m)))
        }
        case x =>
      }
      val reduce = jget(tab, "reduce")
      if (reduce != null) {
        val from = jgetString(reduce, "from")
        toReduce += (to -> (getMap(toReduce, to) + (from -> reduce)))
        fromReduce += (from -> (getMap(fromReduce, from) + (to -> reduce)))
      }
    }

    var tables = new TreeMap[String, TableConfig]()
    for (tab <- tabs) {
      val name = jgetString(tab, "name")
      val (hasPrefix,prefix) = jget(tab, "prefix") match {
        case a: JsonArray => (true,a)
        case obj: JsonObject => (true,JsonArray(obj))
        case x => (false,JsonArray())
      }
      val info = TableConfig(name, hasPrefix, prefix, getMap(toMap, name), getMap(fromMap, name), getMap(toReduce, name), getMap(fromReduce, name))
      tables += (name -> info)
    }

    var servers = Map[String, ServerConfig]()
    // ringName -> RingConfig
    var rings = Map[String, RingConfig]()
    for (ring <- jgetArray(config, "rings")) {
      val ringName = jgetString(ring, "name")
      var ringMap = Map[String, NodeConfig]()
      var ringSeq = List[String]()
      val nodes = jgetArray(ring, "nodes")
      for ((node, pos) <- nodes.zipWithIndex) {
        val name = jgetString(node, "name")
        val host = jget(node, "host") match {
          case null => "127.0.0.1"
          case n: Json => jgetString(n)
        }
        val port = jget(node, "port") match {
          case null => 8011
          case n: Int => n
        }
        val serverConfig = servers.get(host + ":" + port) match {
          case Some(sconf) => sconf
          case None => {
            val sconf = new ServerConfig(host, port)
            servers += (sconf.name -> sconf)
            sconf
          }
        }
        val nodeConfig = new NodeConfig(ringName, name, serverConfig, pos)

        val nodeName = nodeConfig.name
        ringMap += (nodeName -> nodeConfig)
        ringSeq = ringSeq :+ nodeName
        //pos = pos + 1
      }
      val ringConfig = RingConfig(ringName, ringMap, ringSeq)
      rings += (ringName -> ringConfig)
    }
    new DatabaseConfig(databaseName, rings, tables, servers)
  }
}

// Note DatabaseConfig objects are immutable
private[persist] class DatabaseConfig(
  val name: String,
  val rings: Map[String, RingConfig],
  val tables: TreeMap[String, TableConfig],
  val servers: Map[String, ServerConfig]) {

  val emptyMap = Map[String, Json]()

  // TODO move to DatabaseMap and Send
  def getRef(system: ActorSystem, ringName: String, nodeName: String, tableName: String): ActorRef = {
    val nodeInfo = rings(ringName).nodes(nodeName)
    val host = nodeInfo.server.host
    val port = nodeInfo.server.port
    val ref: ActorRef = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" +
      name + "/" + ringName + "/" + nodeName + "/" + tableName)
    ref
  }

  def addTable(tableName: String): DatabaseConfig = {
    val tableConfig = TableConfig(tableName, false, JsonArray(), emptyMap, emptyMap, emptyMap, emptyMap)
    val tables1 = tables + (tableName -> tableConfig)
    new DatabaseConfig(name, rings, tables1, servers)
  }

  def deleteTable(tableName: String): DatabaseConfig = {
    val tables1 = tables - tableName
    new DatabaseConfig(name, rings, tables1, servers)
  }

  def addNode(ringName: String, nodeName: String, host: String, port: Int): DatabaseConfig = {
    val serverName = host + ":" + port
    val servers1 = if (servers.contains(serverName)) servers else servers + (serverName -> ServerConfig(host, port))
    val ring = rings(ringName)
    val nodes = ring.nodes
    val nodeSeq = ring.nodeSeq
    val nodeConfig = NodeConfig(ringName, nodeName, servers1(serverName), nodeSeq.size)
    val nodes1 = nodes + (nodeName -> nodeConfig)
    val seq1 = (nodeName :: (nodeSeq.reverse)).reverse
    val ring1 = RingConfig(ring.name, nodes1, seq1)
    val rings1 = rings + (ringName -> ring1)
    new DatabaseConfig(name, rings1, tables, servers1)
  }

  def deleteNode(ringName: String, nodeName: String): DatabaseConfig = {
    val ring = rings(ringName)
    val nodes = ring.nodes
    val nodeSeq = ring.nodeSeq
    val nodes0 = nodes - nodeName
    var nodes1 = nodes0
    for (((nodeName1, nodeConfig), pos) <- nodes0.zipWithIndex) {
      if (nodeConfig.pos != pos) {
        val nodeConfig1 = NodeConfig(ringName, nodeName1, nodeConfig.server, pos)
        nodes1 += (nodeName1 -> nodeConfig1)
      }
    }
    val seq1 = nodeSeq.filterNot(_ == nodeName)
    val ring1 = RingConfig(ring.name, nodes1, seq1)
    val rings1 = rings + (ringName -> ring1)
    
    var servers1 = Map[String,ServerConfig]()
    for ((ringName,ringConfig)<- rings1) {
      for ((nodeName, nodeConfig) <- ringConfig.nodes) {
        servers1 += (nodeConfig.server.name-> nodeConfig.server)
      }
    }
    new DatabaseConfig(name, rings1, tables, servers1)
  }

  def addRing(ringName: String, nodesArray: JsonArray):DatabaseConfig = {
    var servers1 = servers
    var nodes1 = Map[String,NodeConfig]()
    var nodeSeq1 = List[String]()
    for ((node,pos) <- nodesArray.zipWithIndex) {
      val nodeName = jgetString(node, "name")
      val host = jgetString(node, "host")
      val port = jgetInt(node, "port")
      val serverName = host + ":" + port
      nodeSeq1 = nodeName +: nodeSeq1
      if (!servers1.contains(serverName)) {
        servers1 += (serverName -> ServerConfig(host, port))
      }
      nodes1 += nodeName -> NodeConfig(ringName, nodeName, servers1(serverName), pos) 
    }
    val ringConfig = RingConfig(ringName, nodes1, nodeSeq1.reverse, false)
    val rings1 = rings + (ringName -> ringConfig)
    new DatabaseConfig(name, rings1, tables, servers1)
  }
  
  def deleteRing(ringName: String):DatabaseConfig = {
    val rings1 = rings - ringName
    var servers1 = Map[String,ServerConfig]()
    for ((ringName,ringConfig)<- rings1) {
      for ((nodeName, nodeConfig) <- ringConfig.nodes) {
        servers1 += (nodeConfig.server.name-> nodeConfig.server)
      }
    }
    new DatabaseConfig(name, rings1, tables, servers1)
  }

  def enableRing(ringName: String, avail:Boolean) {
    val rc = rings(ringName)
    val rings1 = rings + (ringName -> RingConfig(ringName, rc.nodes, rc.nodeSeq, avail))
    new DatabaseConfig(name, rings1, tables, servers)
  }

  def toJson: Json = {
    var jtables = JsonArray()
    for ((tableName, tableConfig) <- tables) {
      var jtable = JsonObject(("name" -> tableName))
      var jmap = JsonArray()
      for ((from, jm) <- tableConfig.toMap) {
        jmap = jm +: jmap
      }
      if (jsize(jmap) == 1) {
        jtable += ("map" -> jget(jmap, 0))
      } else if (jsize(jmap) > 1) {
        jtable += ("map" -> jmap.reverse)
      }
      var jreduce = JsonArray()
      for ((from, jr) <- tableConfig.toReduce) {
        jreduce = jr +: jreduce
      }
      if (jsize(jreduce) == 1) {
        jtable += ("reduce" -> jget(jreduce, 0))
      } else if (jsize(jreduce) > 1) {
        jtable += ("reduce" -> jreduce.reverse)
      }
      if (tableConfig.hasPrefix) {
        jtable += ("prefix" -> tableConfig.prefix)
      }
      jtables = jtable +: jtables
    }
    var jrings = JsonArray()
    for ((ringName, ringConfig) <- rings) {
      if (true || ringConfig.available) {
        var jnodes = JsonArray()
        for ((nodeName, nodeConfig) <- ringConfig.nodes) {
          jnodes = JsonObject(("name" -> nodeName), ("host" -> nodeConfig.server.host), ("port" -> nodeConfig.server.port)) +: jnodes
        }
        jrings = JsonObject("name" -> ringName, "nodes" -> jnodes.reverse) +: jrings
      }
    }
    JsonObject(("tables" -> jtables.reverse), ("rings" -> jrings.reverse))
  }
}