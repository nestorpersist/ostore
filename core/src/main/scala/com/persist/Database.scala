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
import JsonOps._
import akka.actor.ActorRef
import akka.actor.Props
import akka.dispatch.DefaultPromise
import akka.util.Timeout
import akka.dispatch.Await
import akka.util.duration._
import akka.pattern._
import akka.dispatch.ExecutionContext

class Database private[persist] (system: ActorSystem, databaseName: String, manager: ActorRef) {
  private implicit val timeout = Timeout(5 seconds)
  private lazy implicit val ec = ExecutionContext.defaultExecutionContext(system)

  def allTables: Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    manager ? ("allTables", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def tableInfo(tableName: String, options: JsonObject = emptyJsonObject): Json = {
    var p = new DefaultPromise[Traversable[Json]]
    manager ! ("tableInfo", p, databaseName, tableName, options)
    val v = Await.result(p, 5 seconds)
    v
  }

  def allRings: Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    manager ? ("allRings", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def ringInfo(ringName: String, options: JsonObject = emptyJsonObject): Json = {
    var p = new DefaultPromise[Traversable[Json]]
    manager ! ("ringInfo", p, databaseName, ringName, options)
    val v = Await.result(p, 5 seconds)
    v
  }

  def allNodes(ringName: String): Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    manager ? ("allNodes", p, databaseName, ringName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def nodeInfo(ringName: String, nodeName: String, options: JsonObject = emptyJsonObject): Json = {
    var p = new DefaultPromise[Traversable[Json]]
    manager ! ("nodeInfo", p, databaseName, ringName, nodeName, options)
    val v = Await.result(p, 5 seconds)
    v
  }

  def allServers: Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    manager ? ("allServers", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def serverInfo(serverName: String, options: JsonObject = emptyJsonObject): Json = {
    var p = new DefaultPromise[Traversable[Json]]
    manager ! ("nodeInfo", p, databaseName, serverName, options)
    val v = Await.result(p, 5 seconds)
    v
  }

  // TODO remove
  def addTable(tableName: String) {
    var p = new DefaultPromise[String]
    manager ? ("addTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
  }

  // TODO remove
  def deleteTable(tableName: String) {
    var p = new DefaultPromise[String]
    manager ? ("deleteTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
  }
  
  def addTables(config: Json) {
    var p = new DefaultPromise[String]
    manager ? ("addTables", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }

  def deleteTables(config: Json) {
    var p = new DefaultPromise[String]
    manager ? ("deleteTables", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }
 
  def addNodes(config: Json) {
    var p = new DefaultPromise[String]
    manager ? ("addNodes", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }
  
  def deleteNodes(config: Json) {
    var p = new DefaultPromise[String]
    manager ? ("deleteNodes", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }
  
    
  def addRings(config: Json) {
    var p = new DefaultPromise[String]
    manager ? ("addRings", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }
  
  def deleteRings(config: Json) {
    var p = new DefaultPromise[String]
    manager ? ("deleteRings", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Temporary debugging method.
   */
  def report(tableName: String): Json = {
    var p = new DefaultPromise[Json]
    manager ? ("report", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Temporary debugging method
   */
  def monitor(tableName: String): Json = {
    var p = new DefaultPromise[Json]
    manager ? ("monitor", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def syncTable(tableName: String) = {
    var p = new DefaultPromise[SyncTable]
    manager ? ("syncTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def asyncTable(tableName: String) = {
    var p = new DefaultPromise[AsyncTable]
    manager ? ("asyncTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }
}