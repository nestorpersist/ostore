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
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory

/**
 * This is the client API for accessing OStore databases.
 * The client will set up its own Akka actor system
 * to communicate with servers.
 *
 * @param config (optional) the configuration for this client (see Wiki).
 */
class Client(config: Json) {
  // TODO optional database of client state (list of servers)
  // TODO connect command to add servers 

  /**
   * Construct a client using an empty configuration
   * This will use all the default client configuration values.
   */
  def this() = this(emptyJsonObject)

  private val akkaConfig = ConfigFactory.load
  private var clientConfig = akkaConfig.getConfig("client")
  private val serverConfig = akkaConfig.getConfig("server")
  private val host = jgetString(config, "client", "host")
  private val port = jgetInt(config, "client", "port")
  private var name = jgetString(config, "client", "name")
  private var serverHost = jgetString(config, "server", "host")
  private var serverPort = jgetInt(config, "server", port)

  if (host != "") {
    clientConfig = clientConfig.withValue("akka.remote.netty.hostname", ConfigValueFactory.fromAnyRef(host))
  }
  if (port != 0) {
    clientConfig = clientConfig.withValue("akka.remote.netty.port", ConfigValueFactory.fromAnyRef(port))
  }
  //println("ClientConfig:" + clientConfig)
  //println("ServerConfig:"+serverConfig)
  if (name == "") name = "ostorec"
  /**
   * The actor system used by the client.
   */
  val system = ActorSystem(name, clientConfig)

  if (serverHost == "") {
    serverHost = serverConfig.getString("akka.remote.netty.hostname")
  }
  if (serverPort == 0) {
    serverPort = serverConfig.getInt("akka.remote.netty.port")
  }

  //println("sp:"+serverPort)
  private val manager = system.actorOf(Props(new Manager(serverHost, serverPort)), name = "@manager")

  private implicit val timeout = Timeout(60 seconds)
  private lazy implicit val ec = ExecutionContext.defaultExecutionContext(system)

  /**
   * Stops the client and its associated actors and its actor system.
   */
  def stop() {
    var p = new DefaultPromise[String]
    manager ! ("stop", p)
    val v = Await.result(p, 5 seconds)
    val stopped = gracefulStop(manager, 5 seconds)(system)
    Await.result(stopped, 5 seconds)
    system.shutdown()
  }

  /**
   * Returns the names of all databases that this client knows about.
   *
   * @return the database names.
   */
  def allDatabases(): Iterable[String] = {
    var p = new DefaultPromise[Iterable[String]]
    manager ! ("allDatabases", p)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Test whether a database exists.
   *
   * @param dbName the name of the database.
   * @return true if the databases exists.
   *
   */
  def databaseExists(dbName: String): Boolean = {
    checkName(dbName)
    var p = new DefaultPromise[Boolean]
    manager ! ("databaseExists", p, dbName)
    val v: Boolean = Await.result(p, 5 seconds)
    v
  }

  /**
   * Creates and starts a new database.
   *
   * @param dbName the name of the new database.
   * @param config the configuration for the new database (see Wiki).
   */
  def createDatabase(dbName: String, config: Json) {
    checkName(dbName)
    checkConfig(config)
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
    checkName(dbName)
    var p = new DefaultPromise[String]
    manager ! ("deleteDatabase", p, dbName)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Starts a database. The database must exist and be stopped.
   *
   * @param dbName the name of the database to be started.
   */
  def startDatabase(dbName: String) {
    checkName(dbName)
    var p = new DefaultPromise[String]
    manager ! ("startDatabase", p, dbName)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Stops a database that is started.
   *
   * @param dbName the name of the database must exist and be stopped.
   */
  def stopDatabase(dbName: String) {
    checkName(dbName)
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
  def database(dbName: String, options: JsonObject = emptyJsonObject): Database = {
    checkName(dbName)
    val check = !jgetBoolean("skipCheck")
    if (check && !databaseExists(dbName)) {
      throw new SystemException(Codes.NoDatabase, JsonObject("database" -> dbName))
    }
    var p = new DefaultPromise[Database]
    manager ! ("database", p, dbName, this)
    val database = Await.result(p, 5 seconds)
    database
  }
}