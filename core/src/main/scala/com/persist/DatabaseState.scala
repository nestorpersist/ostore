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

import akka.actor.ActorRef
import Stores._
import JsonOps._
import scala.collection.immutable.TreeMap

private[persist] object DBState {
  val STOP = "stop"
  val STOPPING = "stopping"
  val STARTING = "starting"
  val ACTIVE = "active"
}

private[persist] object DBPhase {
  val EMPTY = ""
  val LOCKED = "locked"
  val PREPARING = "preparing"
  val DEPLOYING = "deploying"
  val DEPLOYED = "deployed"
}

private[persist] class DatabaseStates(store: Store) {

  private val configTable = store.getTable("config")
  private val stateTable = store.getTable("state")

  private var states = TreeMap[String, DatabaseState]()

  def get(databaseName: String): Option[DatabaseState] = states.get(databaseName)

  def all: Iterable[(String, DatabaseState)] = states

  def add(databaseName: String, lock: String): DatabaseState = {
    val state = new DatabaseState(stateTable, configTable, databaseName)
    state.lock = lock // this saves to stateTable
    states += (databaseName -> state)
    state
  }

  def delete(databaseName: String) {
    states -= databaseName
    stateTable.remove(databaseName)
    configTable.remove(databaseName)
  }

  def restore() {
    var done = false
    var key = stateTable.first()
    do {
      key match {
        case Some(databaseName: String) => {
          val state = new DatabaseState(stateTable, configTable, databaseName)
          state.restore
          states += (databaseName -> state)
          key = stateTable.next(databaseName, false)
        }
        case None => done = true
      }
    } while (!done)
  }

  def close() {
    configTable.close
    stateTable.close
  }
}

private[persist] class DatabaseState(stateTable: StoreTable, configTable: StoreTable, val databaseName: String) {

  // "" means no database present (deleted at unlock)
  private var state1: String = ""

  def state = state1

  def state_=(state: String) {
    state1 = state
    stateTable.put(databaseName, Compact(toJson), NOVAL)
  }

  private var phase1: String = ""

  def phase = phase1

  def phase_=(phase: String) {
    phase1 = phase
  }

  // ref to Database actor
  var ref: ActorRef = null

  // "" or locking guid
  private var lock1: String = ""

  def lock = lock1

  def lock_=(lock: String) {
    if (lock == "") {
      phase1 = DBPhase.EMPTY
    } else {
      phase1 = DBPhase.LOCKED
    }
    lock1 = lock
    stateTable.put(databaseName, Compact(toJson), NOVAL)
  }

  private var config1: DatabaseConfig = null

  private def restoreConfig() {
    configTable.getMeta(databaseName) match {
      case Some(s: String) => {
        if (state1 != "") config1 = DatabaseConfig(databaseName, Json(s))
      }
      case None => config1 = null
    }
  }

  def config = config1

  def config_=(config: DatabaseConfig) {
    config1 = config
    configTable.put(databaseName, Compact(config.toJson), NOVAL)
  }

  private def toJson(): Json = {
    JsonObject("lock" -> lock1, "state" -> state1)
  }

  def restore() {
    // TODO if not stopped start in ????
    stateTable.getMeta(databaseName) match {
      case Some(s) => {
        val j = Json(s)
        lock1 = jgetString(j, "lock")
        state1 = jgetString(j, "state")
        state1 = "stop" // ???
      }
      case None =>
    }
    restoreConfig
  }

}