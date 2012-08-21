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
import Exceptions._

/**
 * This is the API for accessing a specific OStore database.
 * Instances of this class are created by the [[com.persist.Client]] database method.
 */
class Database private[persist] (system: ActorSystem, databaseName: String, manager: ActorRef) {
  private implicit val timeout = Timeout(5 seconds)
  private lazy implicit val ec = ExecutionContext.defaultExecutionContext(system)

  /**
   * Returns the names of all the tables of the database.
   * 
   * @return the table names.
   */
  def allTables(): Iterable[String] = {
    var p = new DefaultPromise[Iterable[String]]
    manager ! ("allTables", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def tableInfo(tableName: String, options: JsonObject = emptyJsonObject): Json = {
    checkName(tableName)
    var p = new DefaultPromise[Json]
    manager ! ("tableInfo", p, databaseName, tableName, options)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Returns the names of the rings of the database.
   * 
   * @return the ring names.
   */
  def allRings(): Iterable[String] = {
    var p = new DefaultPromise[Iterable[String]]
    manager ! ("allRings", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  def databaseInfo(options:JsonObject=emptyJsonObject):Json = {
    var p = new DefaultPromise[Traversable[Json]]
    manager ! ("databaseInfo", p, databaseName,options)
    val v = Await.result(p, 5 seconds)
    v
  }
  
  def ringInfo(ringName: String, options: JsonObject = emptyJsonObject): Json = {
    checkName(ringName)
    var p = new DefaultPromise[Json]
    manager ! ("ringInfo", p, databaseName, ringName, options)
    val v = Await.result(p, 5 seconds)
    v
  }

  /*
   * Returns the names of all the names of database ring.
   * 
   * @param ringName the name of the ring.
   * @return the node names.
   */
  def allNodes(ringName: String): Iterable[String] = {
    checkName(ringName)
    var p = new DefaultPromise[Iterable[String]]
    manager ! ("allNodes", p, databaseName, ringName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def nodeInfo(ringName: String, nodeName: String, options: JsonObject = emptyJsonObject): Json = {
    checkName(ringName)
    checkName(nodeName)
    var p = new DefaultPromise[Json]
    manager ! ("nodeInfo", p, databaseName, ringName, nodeName, options)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Returns the names of all the servers of a database.
   * 
   * @return the server names.
   */
  def allServers(): Iterable[String] = {
    var p = new DefaultPromise[Iterable[String]]
    manager ! ("allServers", p, databaseName)
    val v = Await.result(p, 5 seconds)
    v
  }

  def serverInfo(serverName: String, options: JsonObject = emptyJsonObject): Json = {
    var p = new DefaultPromise[Json]
    manager ! ("nodeInfo", p, databaseName, serverName, options)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Adds one or more tables to the database.
   * The Json configuration specifies the new tables.
   * The database must
   * not already have tables with the specified new table names.
   *
   *   @param config The configuration (see the Wiki).
   */
  def addTables(config: Json) {
    checkConfig(config)
    var p = new DefaultPromise[String]
    manager ! ("addTables", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Deletes one or more tables from the database.
   * The Json configuration specifies the names of the tables
   * to be removed.
   *
   *   @param config The configuration (see the Wiki).
   */
  def deleteTables(config: Json) {
    checkConfig(config)
    var p = new DefaultPromise[String]
    manager ! ("deleteTables", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Adds one or more nodes to the database.
   * The Json configuration specifies the rings
   * to which nodes are to be added. Each node
   * specifies a host and a port. The database must have
   * rings with the specified ring names.
   * The database must
   * not already have nodes with the specified node names
   * in the specified rings.
   *
   *   @param config The configuration (see the Wiki).
   */
  def addNodes(config: Json) {
    checkConfig(config)
    var p = new DefaultPromise[String]
    manager ! ("addNodes", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Deletes one or more nodes from the database.
   * The Json configuration specifies the names of the rings
   * from which nodes are to be removed and for each ring the name
   * of the nodes to be removed. Each ring must be
   * left with at least one node.
   *
   *   @param config The configuration (see the Wiki).
   */
  def deleteNodes(config: Json) {
    checkConfig(config)
    var p = new DefaultPromise[String]
    manager ! ("deleteNodes", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Adds one or more rings to the database.
   * The Json configuration specifies the new rings
   * and for each ring its nodes. Each node
   * specifies a host and a port. The database must
   * not already have rings with the specified new ring names.
   *
   *   @param config The configuration (see the Wiki).
   */
  def addRings(config: Json) {
    checkConfig(config)
    var p = new DefaultPromise[String]
    manager ! ("addRings", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }

  /**
   * Deletes one or more rings from the database.
   * The Json configuration specifies the names of the rings
   * to be removed. Every node of a specified ring is removed.
   * There must be at least one ring left.
   *
   *   @param config The configuration (see the Wiki).
   */
  def deleteRings(config: Json) {
    checkConfig(config)
    var p = new DefaultPromise[String]
    manager ! ("deleteRings", p, databaseName, config)
    val v = Await.result(p, 5 seconds)
  }

  def replaceNode(config: Json) {
    checkConfig(config)
    println("replaceNode NYI")
  }

  /**
   * Temporary debugging method.
   */
  def report(tableName: String): Json = {
    checkName(tableName)
    var p = new DefaultPromise[Json]
    manager ? ("report", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Temporary debugging method
   */
  def monitor(tableName: String): Json = {
    checkName(tableName)
    var p = new DefaultPromise[Json]
    manager ? ("monitor", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Returns a synchronous API for accessing a
   * database table.
   *
   * @param tableName The name of the table.
   * @return The synchronous API for the named table.
   */
  def table(tableName: String) = {
    checkName(tableName)
    var p = new DefaultPromise[Table]
    manager ? ("table", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }

  /**
   * Returns an asynchronous API for accessing a
   * database table.
   *
   * @param tableName The name of the table.
   * @return The asynchronous API for the named table.
   */
  def asyncTable(tableName: String) = {
    checkName(tableName)
    var p = new DefaultPromise[AsyncTable]
    manager ? ("asyncTable", p, databaseName, tableName)
    val v = Await.result(p, 5 seconds)
    v
  }
}