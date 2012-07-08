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
import scala.collection.immutable.TreeMap
import JsonOps._
import akka.util.Timeout
import akka.pattern._
import akka.util.duration._
import akka.dispatch.Await
import akka.actor.Props
import akka.dispatch.DefaultPromise
import akka.dispatch.ExecutionContext

class Client(system: ActorSystem, host: String = "127.0.0.1", port: Int = 8011) {
  // TODO optional database of client state (list of servers)
  // TODO connect command to add servers 
  
  private val manager = system.actorOf(Props(new Manager(host,port)), name = "@manager")

  private implicit val timeout = Timeout(60 seconds)
  private lazy implicit val ec = ExecutionContext.defaultExecutionContext(system)
  
  def stop() {
    var p = new DefaultPromise[String]
    manager ! ("stop", p)
    val v = Await.result(p, 5 seconds)
    val stopped = gracefulStop(manager, 5 seconds)(system)
    Await.result(stopped, 5 seconds)
  }
  
  // TODO Traverable may not be right result
  def allDatabases():Traversable[String] = {
    var p = new DefaultPromise[Traversable[String]]
    manager ! ("allDatabases", p)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  def databaseInfo(databaseName:String,options:JsonObject=emptyJsonObject):Json = {
    var p = new DefaultPromise[Traversable[Json]]
    manager ! ("databaseInfo", p, databaseName,options)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  def createDatabase(databaseName: String, config: Json) {
    var p = new DefaultPromise[String]
    manager ! ("createDatabase", p, databaseName, config)
    val v = Await.result(p, 60 seconds)
  }

  def deleteDatabase(databaseName: String) {
    var p = new DefaultPromise[String]
    manager ! ("deleteDatabase", p, databaseName)
    val v = Await.result(p, 5 seconds)
  }

  def startDatabase(databaseName: String) {
    var p = new DefaultPromise[String]
    manager ! ("startDatabase", p, databaseName)
    val v = Await.result(p, 5 seconds)
  }

  def stopDataBase(databaseName: String) {
    var p = new DefaultPromise[String]
    manager ! ("stopDatabase", p, databaseName)
    val v = Await.result(p, 5 seconds)
  }

  def database(databaseName: String):Database = {
    var p = new DefaultPromise[Database]
    manager ! ("database", p, databaseName)
    val database = Await.result(p, 5 seconds)
    database
  }
}