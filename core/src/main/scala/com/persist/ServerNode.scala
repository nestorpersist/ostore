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

import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Props
import akka.dispatch.Await
import akka.pattern._
import akka.util.duration._
import akka.dispatch._
import akka.util.Timeout
import scala.collection.immutable.TreeMap
import JsonOps._

private[persist] class ServerNode(databaseName: String, ringName: String, nodeName: String, send: ActorRef, var config:DatabaseConfig, serverConfig: Json, create: Boolean) extends CheckedActor {
  
  private val monitor = context.actorOf(Props(new Monitor(nodeName)), name = "@mon")
  implicit val timeout = Timeout(5 seconds)

  class TableInfo(val name: String, val table: ActorRef)

  private var tables = TreeMap[String, TableInfo]()

  private val path = jgetString(serverConfig, "path")
  private val store = path match {
    case "" => new InMemoryStore(context, nodeName, "", true)
    case _ =>
      val desc = databaseName + "/" + ringName + "/" + nodeName
      val fname = path + "/" + desc
      new Store(context, nodeName, fname, create)
  }
  private val system = context.system

  def newTable(tableName: String) {
    val table = context.actorOf(
      Props(new ServerTable(databaseName, ringName, nodeName, tableName,
        store, monitor, send, config)), name = tableName)
    var f = table ? ("start1")
    Await.result(f, 5 seconds)
    tables += (tableName -> new TableInfo(tableName, table))
  }

  // TODO do in parallel
  for ((tableName,tableConfig)<- config.tables) {
    newTable(tableName)
  }

  def rec = {
    case ("start1") => {
      sender ! Codes.Ok
    }
    case ("start2") => {
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("start2")
        Await.result(f, 5 seconds)
      }
      sender ! Codes.Ok
    }
    case ("stop1") => {
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("stop1")
        Await.result(f, 5 seconds)
      }
      sender ! Codes.Ok
    }
    case ("stop2") => {
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("stop2")
        Await.result(f, 5 seconds)
      }
      val f = monitor ? ("stop")
      Await.result(f, 5 seconds)
      store.close()
      sender ! Codes.Ok
    }
    case ("stopBalance", forceEmpty:Boolean) => {
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("stopBalance", forceEmpty)
        Await.result(f, 5 seconds)
      }
      sender ! Codes.Ok
    }
    case ("startBalance") => {
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("startBalance")
        Await.result(f, 5 seconds)
      }
      sender ! Codes.Ok
    }
    case ("busyBalance") => {
      var code = Codes.Ok
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("busyBalance")
        val code1 = Await.result(f, 5 seconds)
        if (code1 == Codes.Busy) code = Codes.Busy
      }
      sender ! code
    }
    case ("setConfig", config: DatabaseConfig) => {
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("setConfig", config)
        Await.result(f, 5 seconds)
      }
      this.config = config
      sender ! Codes.Ok
    }
    case ("getLowHigh", tableName:String) => {
      val tableInfo = tables(tableName)
      val f = tableInfo.table ? ("getLowHigh")
      val (code:String,result:Json) = Await.result(f, 5 seconds)
      sender ! (Codes.Ok, result)
    }
    case ("setLowHigh", tableName:String, low:String, high:String) => {
      val tableInfo = tables(tableName)
      val f = tableInfo.table ? ("setLowHigh", low, high)
      Await.result(f, 5 seconds)
      sender ! Codes.Ok
    }
    case ("addTable1", tableName:String, config:DatabaseConfig) => {
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("setConfig", config)
        Await.result(f, 5 seconds)
      }
      this.config = config
      newTable(tableName)
      sender ! Codes.Ok
    }
    case ("addTable2", tableName:String) => {
      val tableInfo = tables(tableName)
      val f = tableInfo.table ? ("start2")
      Await.result(f, 5 seconds)
      sender ! Codes.Ok
    }
    case ("deleteTable1", tableName:String, config:DatabaseConfig) => {
      for ((tableName1, tableInfo) <- tables) {
        if (tableName1 != tableName) {
          val f = tableInfo.table ? ("setConfig", config)
          Await.result(f, 5 seconds)
        }
      }
      this.config = config
      val tableInfo = tables(tableName)
      val f = tableInfo.table ? ("stop1")
      Await.result(f, 5 seconds)
      sender ! Codes.Ok
    }
    case ("deleteTable2", tableName:String) => {
      val tableInfo = tables(tableName)
      val f = tableInfo.table ? ("delete2")
      Await.result(f, 5 seconds)
      val stopped = gracefulStop(tableInfo.table, 5 seconds)(system)
      Await.result(stopped, 5 seconds)
      tables = tables - tableName
      sender ! Codes.Ok
    }
    case ("deleteNode") => {
      for ((tableName, tableInfo) <- tables) {
        val f = tableInfo.table ? ("delete2")
        Await.result(f, 5 seconds)
      }
      sender ! Codes.Ok
    }
  }
}
