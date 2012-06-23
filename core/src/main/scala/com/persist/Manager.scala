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

import scala.io.Source
import JsonOps._
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.pattern._
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch._
import akka.actor.Actor
import akka.remote.RemoteClientError
import akka.remote.RemoteClientWriteFailed
import akka.actor.DeadLetter
import akka.actor.Props
import akka.remote.RemoteLifeCycleEvent
import scala.collection.immutable.TreeMap
import scala.collection.JavaConversions._

// DATABASE STATES
// empty (does not exist)
// stopped (no active actors)
// sync  (listening for internal messages only)
// running (only here are user commands accepted and balancing enabled) 

// idle  condition for sync nodes
//      a. no items in transit from node
//      b. no syncs to other rings incomplete

// CREATE
// verify can create on all nodes
// empty => stopped

// DELETE
// stopped => empty

// START
// stopped => sync
// wait for all nodes to be in sync state
// sync=>running

// STOP
// running => sync
// wait for all nodes to become idle
// sync => stopped

class Manager private[persist](system: ActorSystem, client: Client) {

  private implicit val timeout = Timeout(5 seconds)

  def createDatabase(databaseName: String, config: Json) {
    val map = new NetworkMap(system, databaseName, config)
    val conf = DatabaseConfig(databaseName,config)
    var ok = true
    // TODO could be done in parallel
    for (serverInfo <- map.allServers) {
      val server = serverInfo.ref
      val f = server ? ("newDatabase", databaseName, Compact(config))
      val v = Await.result(f, 5 seconds)
      if (v != Codes.Ok) {
        ok = false
        //println("Create failed: " +v + ":" + serverInfo.name) 
      }
    }
    if (ok) {
      client.addDatabase(databaseName, map, conf, "active")
      for (serverInfo <- client.allServers(databaseName)) {
        val server = serverInfo.ref
        val f = server ? ("startDatabase2", databaseName)
        val v = Await.result(f, 5 seconds)
      }
    }
  }

  def deleteDatabase(databaseName: String) {
    val status = client.getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "stop") throw new Exception("database not stopped")
    // TODO could be done in parallel
    for (serverInfo <- client.allServers(databaseName)) {
      val server = serverInfo.ref
      val f = server ? ("deleteDatabase", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    client.removeDatabase(databaseName)
  }

  def startDatabase(databaseName: String) {
    val status = client.getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "stop") throw new Exception("database not stopped")
    // TODO could be done in parallel
    for (serverInfo <- client.allServers(databaseName)) {
      val server = serverInfo.ref
      val f = server ? ("startDatabase1", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    for (serverInfo <- client.allServers(databaseName)) {
      val server = serverInfo.ref
      val f = server ? ("startDatabase2", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    client.setDatabaseStatus(databaseName, "active")
  }

  def stopDataBase(databaseName: String) {
    val status = client.getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "active") throw new Exception("database not active")
    // TODO could be done in parallel
    for (serverInfo <- client.allServers(databaseName)) {
      val server = serverInfo.ref
      val f = server ? ("stopDatabase1", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    for (serverInfo <- client.allServers(databaseName)) {
      val server = serverInfo.ref
      val f = server ? ("stopDatabase2", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    client.setDatabaseStatus(databaseName, "stop")
  }

}