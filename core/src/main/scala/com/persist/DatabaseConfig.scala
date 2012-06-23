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

private[persist] case class ServerConfig(
    val host:String,
    val port:Int) {
  val name = host + ":" + port
}

private[persist] case class RingConfig(
  val name: String,
  // serverName -> ServerConfig
  val nodes: Map[String, NodeConfig],
  // server names
  val nodeSeq: List[String]) {

  def nodePosition(nodeName: String): Int = {
    nodes(nodeName).pos
  }

  def nextNodeName(nodeName: String): String = {
    val pos = nodes(nodeName).pos  + 1
    val pos1 = if (pos == nodeSeq.size) 0 else pos
    nodeSeq(pos1)
  }

  def prevNodeName(nodeName: String): String = {
    val pos = nodes(nodeName).pos  - 1
    val pos1 = if (pos < 0) nodeSeq.size-1 else pos
    nodeSeq(pos1)
  }

}

private[persist] case class NodeConfig(
  val ringName: String,
  val name: String,
  val server:ServerConfig,
  //val host: String,
  //val port: Int,
  val pos: Int) {
  //def serverName: String = {
  //  host + ":" + port
  //}
}

private[persist] case class TableConfig(
  val tableName: String,
  val prefix: JsonArray,
  val toMap: Map[String, Json],
  val fromMap: Map[String, Json],
  val toReduce: Map[String, Json],
  val fromReduce: Map[String, Json])

private[persist] object DatabaseConfig {

  /*
  private def nodeInit(node: Json, ringName: String, defaultHost: String, defaultPort: Int, pos:Int): NodeConfig = {
    val name = jgetString(node, "name")
    val host = jget(node, "host") match {
      case null => defaultHost
      case n: Json => jgetString(n)
    }
    val port = jget(node, "port") match {
      case null => defaultPort
      case n: Int => n
    }
    val nodeConfig = new NodeConfig(ringName, name, host, port, pos)
    nodeConfig
  }
  */
  
  private def getMap(map:Map[String,Map[String,Json]],name:String):Map[String,Json] = {
    if (! map.contains(name)) { Map[String,Json]() } else { map(name) }
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
            val from = jgetString(map, "from")
            toMap = toMap + (to -> (getMap(toMap,to) + (from -> m)))
            fromMap = fromMap + (from -> (getMap(fromMap,from) + (to -> m)))
          }
        }
        case m: JsonObject => {
          val from = jgetString(map, "from")
          toMap = toMap + (to -> (getMap(toMap,to) + (from -> m)))
          fromMap = fromMap + (from -> (getMap(fromMap,from) + (to -> m)))
        }
        case x =>
      }
      val reduce = jget(tab, "reduce")
      if (reduce != null) {
        val from = jgetString(reduce, "from")
        toReduce = toReduce + (to -> (getMap(toReduce,to) + (from -> reduce)))
        fromReduce = fromReduce + (from -> (getMap(fromReduce,from) + (to -> reduce)))
      }
    }

    var tables = new TreeMap[String, TableConfig]()
    for (tab <- tabs) {
      val name = jgetString(tab, "name")
      val prefix = jget(tab, "prefix") match {
        case a: JsonArray => a
        case obj: JsonObject => JsonArray(obj)
        case x => JsonArray()
      }
      val info = TableConfig(name, prefix, getMap(toMap,name), getMap(fromMap,name), getMap(toReduce,name), getMap(fromReduce,name))
      tables = tables + (name -> info)
    }

    var servers = Map[String, ServerConfig]()
    // ringName -> RingConfig
    var rings = Map[String, RingConfig]()
    val defaults = jget(config, "defaults")
    val defaultHost = jgetString(defaults, "host")
    val defaultPort = jgetInt(defaults, "port")
    for (ring <- jgetArray(config, "rings")) {
      val ringName = jgetString(ring, "name")
      var ringMap = Map[String, NodeConfig]()
      var ringSeq = List[String]()
      var pos: Int = 0
      val nodes = jgetArray(ring, "nodes")
      for (node <- nodes) {
        val name = jgetString(node, "name")
        val host = jget(node, "host") match {
          case null => defaultHost
          case n: Json => jgetString(n)
        }
        val port = jget(node, "port") match {
          case null => defaultPort
          case n: Int => n
        }
        val serverConfig = servers.get(host + ":" + port) match {
          case Some(sconf) => sconf
          case None => {
            val sconf = new ServerConfig(host,port)
            servers += (sconf.name -> sconf)
            sconf
          }
        }
        //val serverConfig = new ServerConfig(host,port)
        val nodeConfig = new NodeConfig(ringName, name, serverConfig, pos)
    
        val nodeName = nodeConfig.name
        ringMap = ringMap + (nodeName -> nodeConfig)
        ringSeq = ringSeq :+ nodeName
        pos = pos + 1
      }
      val ringConfig = RingConfig(ringName, ringMap, ringSeq)
      rings = rings + (ringName -> ringConfig)
    }
    new DatabaseConfig(databaseName, rings, tables, servers)
  }
}

// Note DatabaseConfig objects are immutable
private[persist] class DatabaseConfig(
  val name: String,
  val rings: Map[String, RingConfig],
  val tables: Map[String, TableConfig],
  val servers: Map[String, ServerConfig]) {

  def getRef(system:ActorSystem, ringName:String, nodeName:String, tableName:String):ActorRef = {
    val nodeInfo = rings(ringName).nodes(nodeName)
    val host = nodeInfo.server.host
    val port = nodeInfo.server.port
    val ref: ActorRef = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" + 
            name + "/" + ringName + "/" + nodeName + "/" + tableName)
    ref
  }
  
  def toJson: Json = {
    // TODO
    JsonObject()
  }
}