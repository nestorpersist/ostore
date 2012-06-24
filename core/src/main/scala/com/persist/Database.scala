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

class Database private[persist](system: ActorSystem, databaseName: String ,config:DatabaseConfig) {
  private implicit val timeout = Timeout(5 seconds)
  private val send = system.actorOf(Props(new Send(system,config)))
  private val f = send ? ("start")
  Await.result(f,5 seconds)
  private implicit val executor = system.dispatcher
  
  private[persist] def stop() {
    val f = send ? ("stop")
    Await.result(f, 5 seconds)
    val f1 = gracefulStop(send, 5 seconds)(system)
    Await.result(f1, 5 seconds)
  }
  
  // TODO get from server
  def allTables:Traversable[String] = config.tables.keys
  
  // TODO 
  def tableInfo(tableName:String,options:JsonObject=emptyJsonObject):Json = null
  
  def allRings:Traversable[String] = config.rings.keys
  
  // TODO
  def ringInfo(ringName:String,options:JsonObject=emptyJsonObject):Json = null
  
  def allNodes(ringName:String):Traversable[String] = config.rings(ringName).nodes.keys

  // TODO
  def nodeInfo(ringName:String,nodeName:String,options:JsonObject=emptyJsonObject):Json = null
  
  def allServers:Traversable[String] = config.servers.keys
  
  // TODO
  def serverInfo(serverName:String,option:JsonObject=emptyJsonObject):Json = null
  
  // TODO
  def addTable(tableName:String) {
    
  }

  // TODO
  def deleteTable(tableName:String) {
    
  }
  
  // TODO (add,remove) (nodes,rings)
  
  def syncTable(tableName: String) = new SyncTable(tableName, system, config, send)

  def asyncTable(tableName: String) = new AsyncTable(tableName, system, send)
}