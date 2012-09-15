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
import com.typesafe.config.ConfigFactory
import com.persist.JsonOps._

/**
 * The Check object contains code for checking the consistency of 
 * an OStore database and fixing any problems it finds.
 * 
 * It is not yet available.
 */
object Check {
  // TODO this code is slow, lots of optimizations possible
  // TODO be able to run continuously as background operation

  private val now = System.currentTimeMillis()

  private def checkPair(i1: Option[Json], i2: Option[Json], path: String, ringName: String, ringName1: String) {
    def pair(p1: String, p2: String) = "[" + p1 + "," + p2 + "]"
    def path1 = path + pair(ringName, ringName1)
    (i1, i2) match {
      case (Some(vc1), Some(vc2)) => {
        val v1 = jget(vc1, "v")
        val c1 = jget(vc1, "c")
        val d1 = jgetBoolean(vc1, "d")
        val v2 = jget(vc2, "v")
        val c2 = jget(vc2, "c")
        val d2 = jgetBoolean(vc2, "d")
        if (d1 != d2) {
          println("Inconsistent deletion " + path1 + " " + pair(d1.toString(), d2.toString()))
        } else if (d1 && d2) {
          // OK and deleted
          println("OK (deleted) " + path1)
        } else if (ClockVector.compare(c1, c2) != '=') {
          println("Clock vectors don't match " + path1 + " " + pair(Compact(c1), Compact(c2)))
        } else if (Compact(v1) != Compact(v2)) {
          println("Values don't match " + path1 + " " + pair(Compact(v1), Compact(v2)))
        } else {
          // OK
          println("OK " + path1)
        }
      }
      case (None, _) => {
        println("First item not found" + path1)
      }
      case (_, None) => {
        println("Second item not found" + path1)
      }
    }
  }

  private def checkItem(database: Database, table: Table, databaseName: String, tableName: String, ringName: String, key: JsonKey) {
    // TODO deal with MR consistency
    val path = "/" + databaseName + "/" + tableName + "/" + Compact(key)
    val info = table.get(key, JsonObject("get" -> "vcde", "ring" -> ringName))
    val e = info match {
      case Some(info1) => {
        jgetLong(info1, "e")
      }
      case None => 0
    }
    if (e != 0 && e <= now) {
      table.delete(key)
      println("Deleting expired item " + path)
    } else {

      for (ringName1 <- database.allRings) {
        if (ringName1 != ringName) {
          val info1 = table.get(key, JsonObject("get" -> "vcd", "ring" -> ringName1))
          checkPair(info, info1, path, ringName, ringName1)
        }
      }
    }
  }

  private def checkTable(database: Database, databaseName: String, tableName: String) {
    // TODO clean up tombstones
    // TODO merge iterate over all rings
    val table = database.table(tableName)
    for (ring <- database.allRings) {
      // TODO do multiple items in parallel
      for (key <- table.all(JsonObject("ring" -> ring))) {
        checkItem(database, table, databaseName: String, tableName: String, ring, key: JsonKey)
      }
    }
  }

  private def checkDatabase(client: Client, databaseName: String) {
    // TODO make sure databaseexists and is active
    // TODO make sure all servers are reachable
    val database = client.database(databaseName)
    for (tableName <- database.allTables) {
      val info = database.tableInfo(tableName,JsonObject("get"->"r"))
      if (! jgetBoolean(info,"r")) {
        checkTable(database, databaseName, tableName)
      }
    }
  }

  /**
   * This method allow the check program to be run from the command line.
   * In SBT type 
   * 
   * run-main com.persist.Check
   */
  def main(args: Array[String]) {
    // TODO support a configuration
    // TODO other command line options
    // TODO scala callable method(s)
    val client = new Client()
    for (databaseName <- client.allDatabases) {
      checkDatabase(client, databaseName)
    }
    client.stop()
  }

}
