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

  implicit val timeout = Timeout(5 seconds)

  class DbInfo(val databaseName: String, val map: NetworkMap, config:DatabaseConfig, var status: String) {
    val database = new Database(system, databaseName, map, config)
  }

  // TODO use software trans memory or agent to sync???
  var databases = new TreeMap[String, DbInfo]()

  val server = system.actorFor("akka://ostore@" + host + ":" + port + "/user/@svr")
  val f = server ? ("databases")
  val (code: String, s: String) = Await.result(f, 5 seconds)
  val dblist = jgetArray(Json(s))
  for (db <- dblist) {
    val databaseName = jgetString(db, "name")
    // TODO make sure name not already present
    val state = jgetString(db, "state")
    val f = server ? ("database", databaseName)
    val (code: String, s: String) = Await.result(f, 5 seconds)
    val config = Json(s)
    val map = new NetworkMap(system, databaseName, config)
    val conf = DatabaseConfig(databaseName, config)
    addDatabase(databaseName, map, conf, state)
  }
  
  def stop() {
    for ((databaseName,info)<-databases) {
      info.database.stop()
    }
  }

  def listDatabases(): Json = {
    var result = JsonArray()
    for ((name, dbinfo) <- databases) {
      val db = JsonObject("name" -> name, "status" -> dbinfo.status)
      result = db +: result
    }
    result.reverse
  }

  def allServers(databaseName: String) = databases(databaseName).map.allServers

  def getDatabaseStatus(databaseName: String): String = {
    // TODO get status from server???
    if (!databases.contains(databaseName)) {
      "none"
    } else {
      databases(databaseName).status
    }
  }

  def setDatabaseStatus(databaseName: String, status: String) {
    databases(databaseName).status = status
  }

  def addDatabase(databaseName: String, map: NetworkMap, config:DatabaseConfig, status: String) {
    val info = new DbInfo(databaseName, map, config, status)
    databases += (databaseName -> info)
  }

  def removeDatabase(databaseName: String) {
    databases(databaseName).database.stop()
    databases -= databaseName
  }

  def manager() = new Manager(system, this)
  def database(databaseName: String) = databases(databaseName).database
}