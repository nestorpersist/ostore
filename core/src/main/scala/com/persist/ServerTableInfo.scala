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

import akka.actor.Actor
import akka.actor.ActorRef
import JsonOps._

private[persist] trait ServerTableInfoComponent { this: ServerTableAssembly =>
  val info: ServerTableInfo
  class ServerTableInfo(val databaseName: String, val ringName: String, val nodeName: String, val tableName: String,
    initConfig:DatabaseConfig, val send: ActorRef, val store: Store, val monitor:ActorRef) {
    
    var config:DatabaseConfig = initConfig

    val absentMetaS = """{"c":{},"d":true}"""

    val storeTable = store.getTable(tableName)
    
    val uidGen = new UidGen
      
    val (initLow: String, initHigh: String) = if (store.create) {
      val ringConfig = config.rings(ringName)
      val pos = ringConfig.nodePosition(nodeName)
      val nextPos = ringConfig.nodePosition(ringConfig.nextNodeName(nodeName))
      //val ringInfo = map.ringInfo(ringName)
      //val nodeInfo = map.nodeInfo(ringInfo, nodeName)
      //val tableInfo = map.nodeTableInfo(nodeInfo, tableName)
      //val (low: String, high: String) = tableInfo.range.get()
      val low = keyEncode(pos)
      val high = keyEncode(nextPos)
      storeTable.put("!low", low)
      storeTable.put("!high", high)
      (low, high)
    } else {
      val low = storeTable.get("!low") match {
        case Some(s: String) => s
        case None => ""
      }
      val high = storeTable.get("!high") match {
        case Some(s: String) => s
        case None => ""
      }
      val clean = storeTable.get("!clean") match {
        case Some(s: String) => s
        case None => "false"
      }
      if (clean != "true") {
        val desc = databaseName + "/" + ringName + "/" + nodeName + "/" + tableName
        println("Shutdown of " + desc + " was not clean")
      }
      (low, high)
    }
    
    storeTable.put("!clean", "false")
    
    // current node
    var low: String = initLow
    var high: String = initHigh

    class StoreRange(low: String, high: String, singleServer: Boolean) {

      private def range(low: String, includeLow: Boolean, high: String, includeHigh: Boolean, body: String => Unit): Unit = {
        var next = low
        var include = includeLow
        var options = JsonObject()
        while (true) {
          ops.getNext(next, include, true, options) match {
            case Some((n: String, n1: String)) => {
              if (singleServer) {
              } else if (includeHigh) {
                if (n > high) return
              } else {
                if (n >= high) return
              }
              body(n)
              next = n
              include = false
            }
            case None => {
              return
            }
          }
        }
      }

      def foreach(body: String => Unit): Unit = {
        if (info.storeTable.size() == 0) return
        if (high < low) {
          val Some(first) = info.storeTable.first()
          val Some(last) = info.storeTable.last()
          range(low, true, last, true, body)
          range(first, true, high, false, body)
        } else {
          range(low, true, high, false, body)
        }
      }
    }

    def range(low: String, high: String, singleServer: Boolean) = {
      new StoreRange(low, high, singleServer)
    }
  }
}
