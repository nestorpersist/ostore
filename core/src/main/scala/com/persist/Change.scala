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
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Await
import JsonOps._

private[persist] class Change(config: DatabaseConfig, optc: Option[Client], messaging: ActorRef) extends CheckedActor with ActorLogging {

  private implicit val timeout = Timeout(5 seconds)

  private val optdb = optc match {
    case Some(c) => Some(c.database(config.name))
    case None => None
  }

  def rec = {
    case ("noRing", uid: Long, ringName: String) => {
      optdb match {
        case Some(database) => {
          if (database.allRings().exists(_ == ringName)) {
            var nodes = emptyJsonArray
            for (nodeName <- database.allNodes(ringName)) {
                val request = JsonObject("get"->"hp")
                val info = database.nodeInfo(ringName, nodeName, request)
                val host = jgetString(info,"h")
                val port = jgetInt(info, "p")
                nodes = JsonObject("host"->host, "port"->port) +: nodes
            }
            val f = messaging ? ("addRing", ringName, nodes.reverse)
            Await.result(f, 5 seconds)
            messaging ! ("continue", uid)
          } else {
            messaging ! ("fail", uid, Codes.NoRing, JsonObject("ring"->ringName))
          }
        }
        case None => {
          messaging ! ("fail", uid, Codes.NoRing, JsonObject("ring"->ringName))
        }
      }
    }
    case ("noTable", uid: Long, tableName: String) => {
      optdb match {
        case Some(database) => {
          if (database.allTables().exists(_ == tableName)) {
            val f = messaging ? ("addTable", tableName)
            Await.result(f, 5 seconds)
            messaging ! ("continue", uid)
          } else {
            messaging ! ("fail", uid, Codes.NoTable, JsonObject("table"->tableName))
          }
        }
        case None => {
          messaging ! ("fail", uid, Codes.NoTable, JsonObject("table"->tableName))
        }
      }
    }
    case ("noNode", uid:Long, ringName:String, nodeName:String) => {
      optdb match {
        case Some(database) => {
          if (database.allNodes(ringName).exists(_ == nodeName)) {
            val request = JsonObject("get"->"bhp")
            val info = database.nodeInfo(ringName, nodeName, request)
            val prevNodeName = jgetString(info, "b")
            val host = jgetString(info, "h")
            val port =jgetInt(info, "p")
            val f = messaging ? ("addNode", prevNodeName, nodeName, host, port)
            Await.result(f, 5 seconds)
            messaging ! ("continue", uid)
          } else {
            messaging ! ("fail", uid, Codes.NoNode, JsonObject("ring"->ringName,"node"->nodeName))
          }
        }
        case None => {
          messaging ! ("fail", uid, Codes.NoNode, JsonObject("ring"->ringName,"node"->nodeName))
        }
      }      
    }
  }

}