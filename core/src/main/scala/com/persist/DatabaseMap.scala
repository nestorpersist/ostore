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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import scala.collection.immutable.TreeMap
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet

private[persist] case class RingMap(
    var tables: HashMap[String, TableMap],
    // TODO nodeconfig => nodeMap
    // NodeMap => TableNodeMap
    var nodes: Map[String, NodeMap],
    // server names
    val nodeSeq: List[String]) {
  
  def nodePosition(nodeName: String): Int = {
    nodes(nodeName).pos
  }

  def nextNodeName(nodeName: String): String = {
    val pos = nodes(nodeName).pos + 1
    val pos1 = if (pos == nodeSeq.size) 0 else pos
    nodeSeq(pos1)
  }

  def prevNodeName(nodeName: String): String = {
    val pos = nodes(nodeName).pos - 1
    val pos1 = if (pos < 0) nodeSeq.size - 1 else pos
    nodeSeq(pos1)
  }

}

private[persist] case class TableMap(
  // all nodes
  var nodes: HashMap[String, TableNodeMap]) {

  // low -> Set(nodeName)
  // only contains nodes with known low and high values
  var keys = new TreeMap[String, HashSet[String]]()
}

private[persist] case class NodeMap(val databaseName: String,val ringName: String, val nodeName: String,
  var pos: Int, val host: String, val port: Int)

private[persist] case class TableNodeMap(val tableName:String, val node:NodeMap) {
  var known: Boolean = false
  var low: String = ""
  var high: String = ""
   
  private var ref: ActorRef = null

  def getRef(system: ActorSystem): ActorRef = {
    if (ref == null) {
      ref = system.actorFor("akka://ostore@" + node.host + ":" + node.port + "/user/" +
        node.databaseName + "/" + node.ringName + "/" + node.nodeName + "/" + tableName)
    }
    ref
  }
}

private[persist] object DatabaseMap {
  
  def addTable(map:DatabaseMap, tableName:String)  {
    for ((ringName, ringMap) <- map.rings) {
      var nodes = HashMap[String, TableNodeMap]()
      for ((nodeName,nodeMap) <- ringMap.nodes) {
        nodes += (nodeName -> TableNodeMap(tableName, nodeMap))
      }
      ringMap.tables += (tableName -> TableMap(nodes))
    }
  }
  
  def apply(config: DatabaseConfig): DatabaseMap = {
    val databaseName = config.name
    var rings = HashMap[String, RingMap]()
    for ((ringName, ringConfig) <- config.rings) {
      var nodes = Map[String, NodeMap]()
      var nodeSeq = ringConfig.nodeSeq
      for ((nodeName,nodeConfig) <- ringConfig.nodes) {
        val nodeMap = NodeMap(databaseName, ringName, nodeName, nodeConfig.pos, 
            nodeConfig.server.host: String, nodeConfig.server.port: Int)
        nodes += (nodeName -> nodeMap)
      }
      var tables = HashMap[String, TableMap]()
      /*
      for ((tableName, tableConfig) <- config.tables) {
        var tnodes = HashMap[String, TableNodeMap]()
        for ((nodeName, nodeConfig) <- ringConfig.nodes) {
          val nodeMap = nodes(nodeName)
          tnodes += (nodeName -> TableNodeMap(tableName, nodeMap))
        }
        tables += (tableName -> TableMap(tnodes))
      }
      */
      rings += (ringName -> RingMap(tables,nodes,nodeSeq))
    }
    val map = new DatabaseMap(databaseName, rings)
    for ((tableName, tableConfig) <- config.tables) {
        addTable(map, tableName)
    }
    map
  }
}

/* DatabaseMap contains mutable data and should be used by only
 * a single thread (typically actor Send)
 */
// TODO get rid of config, move relevant ops to here
private[persist] class DatabaseMap(val databaseName: String, val rings: HashMap[String, RingMap]) {

  private def nodeMax(nodes: HashMap[String, TableNodeMap], n1: String, n2: String): String = {
    if (n1 == "") {
      n2
    } else {
      val info1 = nodes(n1)
      val info2 = nodes(n2)
      if (info1.low > info1.high && info2.low > info2.high) {
        if (info1.high > info2.high) {
          n1
        } else {
          n2
        }
      } else if (info1.low > info1.high) {
        n1
      } else if (info2.low > info2.high) {
        n2
      } else if (info1.high > info2.high) {
        n1
      } else {
        n2
      }
    }
  }

  private def bestFit(tableMap: TableMap, key: String, less: Boolean): String = {
    // based on low of nodes with known range
    val keys = tableMap.keys
    val nodes = tableMap.nodes
    if (keys.isEmpty) {
      // we know nothing about any nodes,
      // so lets just pick one to get started
      val (nodeName, nodeMap) = nodes.head
      nodeName
    } else {
      val initialKeys = if (less) { keys.until(key) } else { keys.to(key) }
      val set = initialKeys.lastOption match {
        case Some((low, set)) => {
          set
        }
        case None => {
          val (low, set) = keys.last
          set
        }
      }
      var result: String = ""
      for (nodeName <- set) {
        result = nodeMax(nodes, result, nodeName)
      }
      result
    }
  }

 private def inNode(node: TableNodeMap, key: String, less: Boolean): Boolean = {
    if (!node.known) {
      true
    } else if (node.low <= node.high) {
      if (less) {
        node.low < key && key <= node.high
      } else {
        node.low <= key && key < node.high
      }
    } else {
      if (less) {
        node.low < key || key <= node.high
      } else {
        node.low <= key || key < node.high
      }
    }
  }

  def get(ringName: String, tableName: String, key: String, less: Boolean): (TableNodeMap, TableNodeMap) = {
    val ringMap = rings(ringName)
    val tmap = ringMap.tables.get(tableName)
    tmap match {
      case Some(tableMap: TableMap) => {
        val nodes = tableMap.nodes
        if (nodes.size == 1) {
          // only one node, so use it
          val (nodeName, n) = nodes.head
          (n, n)
        } else {
          val nodeName = bestFit(tableMap, key, less)
          val n = nodes(nodeName)
          if (inNode(n, key, less)) {
            (n, n)
          } else {
            // possible in-transit 
            val nextNodeName = rings(ringName).nextNodeName(nodeName)
            val next = nodes(nextNodeName)
            (n, next)
          }
        }
      }
      case None => {
        // May or may not exist, so lets
        // pick a node and see if it has the table
        // TODO should log this
        val (nodeName, nodeMap) = rings(ringName).nodes.head
        val n = TableNodeMap(tableName, nodeMap)
        (n, n)
      }
    }
  }

  def setLowHigh(ringName: String, nodeName: String, tableName: String, low: String, high: String) {
    val ringMap = rings(ringName)
    if (ringMap.tables.get(tableName) == None) {
      // Table is new so create it 
      // TODO log it
      DatabaseMap.addTable(this, tableName)
    }
    val tableMap = ringMap.tables(tableName)
    val tableNodeMap = tableMap.nodes(nodeName)
    if (tableNodeMap.known) {
      // remove old value
      val oldSet: HashSet[String] = tableMap.keys.getOrElse(tableNodeMap.low, new HashSet[String]()) - nodeName
      if (oldSet.size == 0) {
        tableMap.keys -= tableNodeMap.low
      } else {
        tableMap.keys += (tableNodeMap.low -> oldSet)
      }
    }
    // insert new value
    val newSet: HashSet[String] = tableMap.keys.getOrElse(low, new HashSet[String]()) + nodeName
    tableMap.keys += (low -> newSet)

    tableNodeMap.low = low
    tableNodeMap.high = high
    tableNodeMap.known = true
  }

}