package com.persist

import JsonOps._
import scala.collection.immutable.TreeMap
import akka.actor.ActorRef
import akka.actor.ActorSystem

case class RingConfig(
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

case class NodeConfig(
  val ringName: String,
  val name: String,
  val host: String,
  val port: Int,
  val pos: Int) {
  def serverName: String = {
    host + ":" + port
  }
}

case class TableConfig(
  val tableName: String,
  val prefix: JsonArray,
  val toMap: Map[String, Json],
  val fromMap: Map[String, Json],
  val toReduce: Map[String, Json],
  val fromReduce: Map[String, Json])

object DatabaseConfig {

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
        //val low = pos
        //val high = if (pos == nodes.size - 1) { 0 } else { pos + 1 }
        //val lows = keyEncode(low)
        //val highs = keyEncode(high)
        val nodeConfig = nodeInit(node, ringName, defaultHost, defaultPort, pos)
        val nodeName = nodeConfig.name
        ringMap = ringMap + (nodeName -> nodeConfig)
        ringSeq = ringSeq :+ nodeName
        pos = pos + 1
      }
      val ringConfig = RingConfig(ringName, ringMap, ringSeq)
      rings = rings + (ringName -> ringConfig)
    }
    new DatabaseConfig(databaseName, rings, tables)
  }
}

// Note DatabaseConfig objects are immutable
class DatabaseConfig(
  val name: String,
  val rings: Map[String, RingConfig],
  val tables: Map[String, TableConfig]) {

  def getRef(system:ActorSystem, ringName:String, nodeName:String, tableName:String):ActorRef = {
    val nodeInfo = rings(ringName).nodes(nodeName)
    val host = nodeInfo.host
    val port = nodeInfo.port
    val ref: ActorRef = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" + 
            name + "/" + ringName + "/" + nodeName + "/" + tableName)
    ref
  }
  
  def toJson: Json = {
    // TODO
    JsonObject()
  }
}