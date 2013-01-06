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
import JsonOps._
import JsonKeys._
import scala.io.Source
import scala.collection.immutable.HashMap

// UNLOCK
// 1. Some in deployed or deploying
//      fwd(if deploying) and unlock
// 2. Some in preparing
//      bwd(if preparing) and unlock
// 3. Some in locked
//      unlock those that are locked

private[persist] class Check(cmd: String, config: Json) {
  // TODO scala callable method(s)
  // TODO be able to run continuously as background operation
  // TODO do expire
  // TODO garbage collection of deleted items and clock vectors
  // TODO cfix map and reduce tables

  private val now = System.currentTimeMillis()
  private val client = new Client(jget(config, "client"))
  val fix = cmd == "fix"
    
  val checkMap = new CheckMap(client, cmd,config)
  val checkReduce = new CheckReduce(client, cmd, config)

  private def checkPair1(database: Database, table: Table, key: JsonKey, i1: Item, i2: Item) {
    def pair(p1: String, p2: String) = "[" + p1 + "," + p2 + "]"
    lazy val path = "/" + database.databaseName + "/" + table.tableName + "/" + Compact(key) + Pair(i1.ringName, i2.ringName)
    (i1.v, i2.v) match {
      case (null, null) => println("Both null" + path) // both missing
      case (null, _) => {
        if (!i2.d) {
          println("First item not found " + path)
          if (fix) {
            table.sync(key, JsonObject("ring" -> i2.ringName))
            println("   Fixed " + path)
          }
        }
      }
      case (_, null) => {
        println("Second item not found" + path)
        if (!i1.d) {
          if (fix) {
            table.sync(key, JsonObject("ring" -> i1.ringName))
            println("   Fixed " + path)
          }
        }
      }
      case (v1, v2) => {
        val cmp = ClockVector.compare(i1.c, i2.c)
        if (cmp != '=') {
          // TODO allow if change is very recent ??
          println("Clock vectors don't match " + path + " " + pair(Compact(i1.c), Compact(i2.c)))
          if (fix) {
            if (cmp != '<') table.sync(key, JsonObject("ring" -> i1.ringName))
            if (cmp != '>') table.sync(key, JsonObject("ring" -> i2.ringName))
            println("   Fixed " + path)
          }
        } else if (i1.d != i2.d) {
          println("Inconsistent deletion " + path + " " + pair(i1.d.toString(), i2.d.toString()))
          if (fix) {
            println("   **** Can't fix: " + path)
          }
        } else if (i1.d && i2.d) {
          // OK and deleted
          //println("OK (deleted) " + path)
        } else if (i1.vs != i2.vs) {
          println("Values don't match " + path + " " + pair(i1.vs, i2.vs))
          if (fix) {
            println("   **** Can't fix: " + path)
          }
        } else {
          // OK
          //println("OK " + path)
        }
      }
    }
  }

  private case class Key(val k: JsonKey, s: String)

  private case class Item(ringName: String, d: Boolean, c: Json, v: Json, vs: String)

  private def next(old: Key, all: Iterator[Json], prev: Key): Key = {
    if (old.s > prev.s) {
      old
    } else if (all.hasNext) {
      val next = all.next
      Key(next, keyEncode(next))
    } else {
      Key(null, "\uffff")
    }
  }

  private def nextItem(database: Database, table: Table, rings: List[String], ringItems: List[Iterator[Json]], keys: List[Key], prev: Key) {
    val newKeys = keys.zip(ringItems).map(x => {
      val (k, all) = x
      next(k, all, prev)
    })
    if (newKeys.exists(_.k != null)) {
      val key = newKeys.fold(Key(null, "\uffff")) { (k1, k2) => if (k1.s < k2.s) k1 else k2 }
      val checkItems = newKeys.zip(rings).map(x => {
        val (k, ringName) = x
        if (k.s == key.s) {
          val info1 = table.get(key.k, JsonObject("get" -> "vcde", "ring" -> ringName))
          info1 match {
            case Some(info) => {
              val d = jgetBoolean(info, "d")
              val c = jget(info, "c")
              val v = jget(info, "v")
              val vs = Compact(v)
              Item(ringName, d, c, v, vs)
            }
            case None => {
              Item(ringName, true, emptyJsonObject, null, "")
            }
          }
        } else {
          Item(ringName, true, emptyJsonObject, null, "")
        }
      })
      checkItems.map { item1 =>
        // TODO deal with expires
        checkItems.map { item2 =>
          if (item1.ringName < item2.ringName) {
            checkPair1(database, table, key.k, item1, item2)
          }
        }
      }
      nextItem(database, table, rings, ringItems, newKeys, key)
    }
  }

  private def allRings(database: Database, table: Table) {
    val rings = database.allRings.toList
    val ringItems = rings.map(ringName => table.all(JsonObject("ring" -> ringName)).iterator)
    val keys = rings.map(ringName => Key(null, ""))
    val prev = Key(null, "")
    nextItem(database, table, rings, ringItems, keys, prev)
  }

  private def checkTable(database: Database, tableName: String) {
    val info = database.tableInfo(tableName, JsonObject("get" -> "tp"))
    val t = jget(info, "t")
    val p = jgetArray(info, "p")
    println("*** TABLE: /" + database.databaseName + "/" + tableName + " ***")
    val table = database.table(tableName)
    if (t == null) {
      // TODO clean up tombstones
      allRings(database, table)
    } else {
      checkMap.checkMaps(database, table, t, p)
      checkReduce.checkReduces(database, table, t)
    }
  }

  def checkDatabase(databaseName: String) {
    val database = client.database(databaseName)
    println("*** DATABASE: /" + databaseName + " ***")
    if (cmd == "state") {
      for (server <- database.allServers) {
        println("*** SERVER: /" + server + " ***")
        val info = client.serverInfo(server, JsonObject("get" -> "hpd", "database" -> databaseName))
        println(Pretty(info))
      }
    } else {
      // TODO make sure databaseexists and is active
      // TODO make sure all servers are reachable
      // TODO verify database is active
      for (tableName <- database.allTables) {
        checkTable(database, tableName)
      }
    }
  }

  private def all {
    for (databaseName <- client.allDatabases) {
      checkDatabase(databaseName)
    }

  }

  private def stop() {
    client.stop()
  }
}

/**
 * The Check object contains code for checking the consistency of
 * an OStore database and fixing any problems it finds.
 */
object Check {

  def params(args: Array[String]): (Array[String], Map[String, String]) = {
    val (named, pos) = args.span(_.contains("="))
    val map = named.foldLeft(HashMap[String, String]())((map, s) => {
      val parts = s.split("=")
      map + (parts(0) -> parts(1))
    })
    (pos, map)
  }

  /**
   * This method allow the check program to be run from the command line.
   * In SBT type
   *
   * '''run-main com.persist.Check'''
   *
   * @param args command line args.
   *   - args(0) command. Either '''check''', '''fix''' or '''check'''.
   *   - args(1) databaseName. If missing do all databases.
   *   - config=path path to config (see wiki). Default is '''config/check.json'''.
   */
  def main(args: Array[String]) {
    val (pos, named) = params(args)
    val path = named.getOrElse("config", "config/check.json")
    val config = Json(Source.fromFile(path).mkString)
    if (pos.size == 0) {
      println("Must specify command")
      return
    }
    val cmd = pos(0)
    cmd match {
      case "check" =>
      case "fix" =>
      case "state" =>
      case x => {
        println("Unknown command:" + x)
        return
      }
    }
    val check = new Check(cmd, config)

    if (pos.size > 1) {
      check.checkDatabase(pos(1))
    } else {
      check.all
    }
    check.stop()
  }

}
