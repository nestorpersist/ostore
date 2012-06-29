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

class Database private[persist](system: ActorSystem, databaseName: String , clientActor:ActorRef) {
  private implicit val timeout = Timeout(5 seconds)
  private lazy implicit val ec = ExecutionContext.defaultExecutionContext(system)
  
  def allTables:Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    clientActor ? ("allTables", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  // TODO 
  def tableInfo(tableName:String,options:JsonObject=emptyJsonObject):Json = null
  
  def allRings:Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    clientActor ? ("allRings", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  // TODO
  def ringInfo(ringName:String,options:JsonObject=emptyJsonObject):Json = null
  
  def allNodes(ringName:String):Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    clientActor ? ("allNodes", p, databaseName, ringName)
    val v = Await.result(p, 5 seconds)
    v
  }

  // TODO
  def nodeInfo(ringName:String,nodeName:String,options:JsonObject=emptyJsonObject):Json = null
  
  def allServers:Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    clientActor ? ("allServers", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  // TODO
  def serverInfo(serverName:String,option:JsonObject=emptyJsonObject):Json = null
  
  def addTable(tableName:String) {
    var p = new DefaultPromise[String]
    clientActor ? ("addTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)

  }

  def deleteTable(tableName:String) {
    var p = new DefaultPromise[String]
    clientActor ? ("deleteTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
  }
  
  
  /**
   * Temporary debugging method.
   */
  def report(tableName: String): Json = {
    var p = new DefaultPromise[Json]
    clientActor ? ("report", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Temporary debugging method
   */
  def monitor(tableName:String): Json = {
    var p = new DefaultPromise[Json]
    clientActor ? ("monitor", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  // TODO (add,remove) (nodes,rings)
  
  // TODO make sure table exists local (if not local check remote)
  def syncTable(tableName: String) = {
    var p = new DefaultPromise[SyncTable]
    clientActor ? ("syncTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }

  // TODO make sure table exists local (if not local check remote)
  def asyncTable(tableName: String) = {
    var p = new DefaultPromise[AsyncTable]
    clientActor ? ("asyncTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }
}