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
import Exceptions._

/**
 * This is the client API for accessing OStore databases.
 * 
 * @param system the ActorSystem that the client should use.
 * @param host the host name the client should visit to get database information at startup.
 * @param port the port on the host to use.
 */
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
  
  /**
   * Returns the names of all databases that this client knows about.
   * 
   * @return the database names.
   */
  def allDatabases():Iterable[String] = {
    var p = new DefaultPromise[Iterable[String]]
    manager ! ("allDatabases", p)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  def databaseExists(dbName:String,options:JsonObject=emptyJsonObject):Boolean = {
    var p = new DefaultPromise[Boolean]
    manager ! ("databaseExists", p, dbName,options)
    val v:Boolean = Await.result(p, 5 seconds)
    v
  }
  
  /*
  def databaseInfo(dbName:String,options:JsonObject=emptyJsonObject):Json = {
    var p = new DefaultPromise[Traversable[Json]]
    manager ! ("databaseInfo", p, dbName,options)
    val v = Await.result(p, 5 seconds)
    v
  }
  */
  
  /**
   * Creates and starts a new database.
   * 
   * @param dbName the name of the new database.
   * @param config the configuration for the new database (see Wiki).
   */
  def createDatabase(dbName: String, config: Json) {
    var p = new DefaultPromise[String]
    manager ! ("createDatabase", p, dbName, config)
    val v = Await.result(p, 5 minutes)
  }

  /**
   * Deletes a database. The database must exist and be stopped.
   * 
   * @param dbName the name of the database to be deleted.
   */
  def deleteDatabase(dbName: String) {
    var p = new DefaultPromise[String]
    manager ! ("deleteDatabase", p, dbName)
    val v = Await.result(p, 5 seconds)
  }

  def startDatabase(dbName: String) {
    var p = new DefaultPromise[String]
    manager ! ("startDatabase", p, dbName)
    val v = Await.result(p, 5 seconds)
  }

  def stopDataBase(dbName: String) {
    var p = new DefaultPromise[String]
    manager ! ("stopDatabase", p, dbName)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Returns an API for a database.
   * 
   * @param dbName the name of the database.
   * @return the API for the named database.
   */
  def database(dbName: String, options:JsonObject=emptyJsonObject):Database = {
    val check = ! jgetBoolean("skipCheck")
    if (check && ! databaseExists(dbName)) {
      throw new SystemException(Codes.ExistDatabase,JsonObject("database"->dbName))
    } 
    var p = new DefaultPromise[Database]
    manager ! ("database", p, dbName)
    val database = Await.result(p, 5 seconds)
    database
  }
}