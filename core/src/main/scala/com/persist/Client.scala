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

class Client(system: ActorSystem, host: String = "127.0.0.1", port: String = "8011") {
  // TODO optional database of client state (list of servers)
  // TODO connect command to add servers 
  // TODO is client API thread safe????

  private implicit val timeout = Timeout(5 seconds)

  private val sendServer = new SendServer(system)


  private[persist] class DbInfo(val databaseName: String, val config:DatabaseConfig, var status: String) {
    val database = new Database(system, databaseName, config)
  }

  // TODO don't pass to RestClient
  private var databases = new TreeMap[String, DbInfo]()

  //private val server = system.actorFor("akka://ostore@" + host + ":" + port + "/user/@svr")
  private val server = sendServer.serverRef(host + ":" + port)
  private val f = server ? ("databases")
  private val (code: String, s: String) = Await.result(f, 5 seconds)
  private val dblist = jgetArray(Json(s))
  for (db <- dblist) {
    val databaseName = jgetString(db, "name")
    // TODO make sure name not already present
    val state = jgetString(db, "state")
    val f = server ? ("database", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val config = Json(s)
    //val map = new NetworkMap(system, databaseName, config)
    val conf = DatabaseConfig(databaseName, config)
    addDatabase(databaseName, conf, state)
  }
  
  def stop() {
    for ((databaseName,info)<-databases) {
      info.database.stop()
    }
  }
  
  // TODO get from server
  def allDatabases():Traversable[String] = databases.keys
  
  // TODO get from server
  def databaseInfo(databaseName:String,options:JsonObject=emptyJsonObject):Json = {
    JsonObject("status" -> getDatabaseStatus(databaseName))
  }

  private[persist] def allServers(databaseName: String) = databases(databaseName).config.servers.keys

  private def getDatabaseStatus(databaseName: String): String = {
    // TODO get status from server???
    if (!databases.contains(databaseName)) {
      "none"
    } else {
      databases(databaseName).status
    }
  }

  private[persist] def setDatabaseStatus(databaseName: String, status: String) {
    databases(databaseName).status = status
  }

  private[persist] def addDatabase(databaseName: String, config:DatabaseConfig, status: String) {
    val info = new DbInfo(databaseName, config, status)
    databases += (databaseName -> info)
  }

  private[persist] def removeDatabase(databaseName: String) {
    databases(databaseName).database.stop()
    databases -= databaseName
  }
  
  def createDatabase(databaseName: String, config: Json) {
    //val map = new NetworkMap(system, databaseName, config)
    val conf = DatabaseConfig(databaseName,config)
    var ok = true
    // TODO could be done in parallel
    for (serverName <- conf.servers.keys) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("newDatabase", databaseName, Compact(config))
      val v = Await.result(f, 5 seconds)
      if (v != Codes.Ok) {
        ok = false
        //println("Create failed: " +v + ":" + serverInfo.name) 
      }
    }
    if (ok) {
      addDatabase(databaseName, conf, "active")
      for (serverName <- conf.servers.keys) {
        val server = sendServer.serverRef(serverName)
        val f = server ? ("startDatabase2", databaseName)
        val v = Await.result(f, 5 seconds)
      }
    }
  }

  def deleteDatabase(databaseName: String) {
    val status = getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "stop") throw new Exception("database not stopped")
    // TODO could be done in parallel
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("deleteDatabase", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    removeDatabase(databaseName)
  }

  def startDatabase(databaseName: String) {
    val status = getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "stop") throw new Exception("database not stopped")
    // TODO could be done in parallel
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("startDatabase1", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("startDatabase2", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    setDatabaseStatus(databaseName, "active")
  }

  def stopDataBase(databaseName: String) {
    val status = getDatabaseStatus(databaseName)
    if (status == "none") throw new Exception("database does not exist")
    if (status != "active") throw new Exception("database not active")
    // TODO could be done in parallel
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("stopDatabase1", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    for (serverName <- allServers(databaseName)) {
      val server = sendServer.serverRef(serverName)
      val f = server ? ("stopDatabase2", databaseName)
      val v = Await.result(f, 5 seconds)
    }
    setDatabaseStatus(databaseName, "stop")
  }

  //def manager() = new Manager(system, this, sendServer)
  def database(databaseName: String) = databases(databaseName).database
}