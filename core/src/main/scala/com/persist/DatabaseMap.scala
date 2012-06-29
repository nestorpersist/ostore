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

// Actor Paths
//       /user/@server
//       /user/database
//       /user/database/@send
//       /user/database/ring/node
//       /user/database/ring/node/@mon
//       /user/database/ring/node/table

private[persist] case class RingMap(var tables: HashMap[String, TableMap])

private[persist] case class TableMap(
  // all nodes
  var nodes: HashMap[String, NodeMap]) {

  // low -> Set(nodeName)
  // only contains nodes with known low and high values
  var keys = new TreeMap[String, HashSet[String]]()
}

private[persist] case class NodeMap(databaseName: String, ringName: String, nodeName: String, tableName: String,
  var posx: Int, host: String, port: Int) {
  var known: Boolean = false
  var low: String = ""
  var high: String = ""
  private var ref: ActorRef = null
  
  def getRef(system: ActorSystem): ActorRef = {
    if (ref == null) {
      ref = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" +
        databaseName + "/" + ringName + "/" + nodeName + "/" + tableName)
    }
    ref
  }
}

private[persist] object DatabaseMap {
  def apply(config: DatabaseConfig): DatabaseMap = {
    val databaseName = config.name
    var rings = HashMap[String, RingMap]()
    for ((ringName, ringConfig) <- config.rings) {
      var tables = HashMap[String, TableMap]()
      for ((tableName, tableConfig) <- config.tables) {
        var nodes = HashMap[String, NodeMap]()
        // TODO replace pos with next node name -- new loop build pos->nodeName
        for ((nodeName, nodeConfig) <- ringConfig.nodes) {
          nodes += (nodeName -> NodeMap(databaseName, ringName, nodeName, tableName, nodeConfig.pos, nodeConfig.server.host, nodeConfig.server.port))
        }
        tables += (tableName -> TableMap(nodes))
      }
      for ((nodeName,nodeConfig) <- ringConfig.nodes) {
        
      }
      rings += (ringName -> RingMap(tables))
    }
    new DatabaseMap(databaseName, rings,config)
  }
}

/* DatabaseMap contains mutable data and should be used by only
 * a single thread (typically actor Send)
 */
// TODO get rid of config, move relevant ops to here
private[persist] class DatabaseMap(val databaseName: String, val rings: HashMap[String, RingMap], val config:DatabaseConfig) {

  private def nodeMax(nodes: HashMap[String, NodeMap], n1: String, n2: String): String = {
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

  private def inNode(node: NodeMap, key: String, less: Boolean): Boolean = {
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

  def get(ringName: String, tableName: String, key: String, less: Boolean): (NodeMap, NodeMap) = {
    val ringMap = rings(ringName)
    val tableMap = ringMap.tables(tableName)
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
        val nextNodeName = config.rings(ringName).nextNodeName(nodeName)
        val next = nodes(nextNodeName)
        (n, next)
      }
    }
  }

  def setLowHigh(ringName: String, nodeName: String, tableName: String, low: String, high: String) {
    val ringMap = rings(ringName)
    val tableMap = ringMap.tables(tableName)
    val nodeMap = tableMap.nodes(nodeName)
    if (nodeMap.known) {
      // remove old value
      val oldSet: HashSet[String] = tableMap.keys.getOrElse(nodeMap.low, new HashSet[String]()) - nodeName
      if (oldSet.size == 0) {
        tableMap.keys -= nodeMap.low
      } else {
        tableMap.keys += (nodeMap.low -> oldSet)
      }
    }
    // insert new value
    val newSet: HashSet[String] = tableMap.keys.getOrElse(low, new HashSet[String]()) + nodeName
    tableMap.keys += (low -> newSet)

    nodeMap.low = low
    nodeMap.high = high
    nodeMap.known = true
  }

}