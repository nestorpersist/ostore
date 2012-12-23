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
  // TODO check and fix map and reduce tables

  private val now = System.currentTimeMillis()
  private val client = new Client(jget(config, "client"))
  val fix = cmd == "fix"
    
  val checkMap = new CheckMap(client, cmd,config)

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

  /*
  private def loc(table: String, key: JsonKey) = table + ":" + Compact(key)
  private def loc(table: String, key: JsonKey, value: Json) = table + ":(" + Compact(key) + "," + Compact(value) + ")"
  private def loc(table: String, pkey: JsonKey, pvalue: Json, key: JsonKey, value: Json) =
    table + ":(" + Compact(pkey) + "," + Compact(pvalue) + ")" +
      ":(" + Compact(key) + "," + Compact(value) + ")"

  private class PrefixVals(prefix: String, table: Table, size: Int) {
    // TODO !!! must specify ring
    val opt = JsonObject("prefixtab" -> prefix)
    private var savek: JsonKey = null
    private var savev: Option[Json] = null
    def get(fullk: JsonKey): (JsonKey, Option[Json]) = {
      val k = jgetArray(fullk).take(size)
      if (k != savek) {
        val v = table.get(k, opt)
        savek = k
        savev = v
      }
      (savek, savev)
    }
  }

  private trait MapperInfo {
    def mapTo(k: JsonKey, v: Json): Traversable[(JsonKey, Json)]
    def fromLoc(k: JsonKey, v: Json): String
  }

  private object MapperInfo {
    def apply(table: Table, mapper: MapReduce.MapAll, map: Json, prefixes: Map[String, Int]): MapperInfo = {
      if (jgetString(map, "act2") != "") {
        new MapperInfo2(table, mapper, map, prefixes)
      } else {
        new MapperInfo1(mapper, map)
      }
    }
  }

  private class MapperInfo2(table: Table, mapper: MapReduce.MapAll, map: Json, prefixes: Map[String, Int]) extends MapperInfo {
    val map2 = mapper.asInstanceOf[MapReduce.Map2]
    val prefix = map2.fromprefix
    val size = prefixes(prefix)
    val pvals = new PrefixVals(map2.fromprefix, table, size)
    def mapTo(k: JsonKey, v: Json) = {
      val (pk, opv) = pvals.get(k)
      opv match {
        case Some(pv:Json) => {
          val m2 = map2.to(pk, pv, k, v)
          m2
        }
        case None => List()
      }
    }
    def fromLoc(k: JsonKey, v: Json) = {
      val (pk, opv) = pvals.get(k)
      loc(prefix, pk, opv.getOrElse(null), k, v)
    }
  }

  private class MapperInfo1(mapper: MapReduce.MapAll, map: Json) extends MapperInfo {
    val from = jgetString(map, "from")
    val map1 = mapper.asInstanceOf[MapReduce.Map]
    def mapTo(k: JsonKey, v: Json) = map1.to(k, v)
    def fromLoc(k: JsonKey, v: Json) = loc(from, k, v)
  }

  def getAllMappers(t: Json) = {
    def getMapper(m: Json): MapReduce.MapAll = {
      val act = jgetString(m, "act")
      val act2 = jgetString(m, "act2")
      if (act != "") {
        MapReduce.getMap(act, m)
      } else if (act2 != "") {
        val from = jgetString(m, "fromprefix")
        MapReduce.getMap2(act2, m, from)
      } else {
        null
      }
    }

    // Map[(toprefix,from),map]
    val mappers1 = jgetArray(t, "map").map(m => ((jgetString(m, "toprefix"), jgetString(m, "from")), getMapper(m))).toMap

    // Map[toprefix,Map[(toprefix,from),map]]
    val mappers2 = mappers1.groupBy { case ((prefix, from), m1) => prefix }

    // Map[toprefix,Map[from,map]]
    val mappers = mappers2.map { case (prefix, m1) => prefix -> m1.map { case ((prefix, from), map) => from -> map } }
    mappers
  }

  private def checkMaps(table: Table, t: Json, p: JsonArray) {
    val mappers = getAllMappers(t)
    val prefixes = p.map(i => (jgetString(i, "name") -> jgetInt(i, "size"))).toMap

    for (ring <- table.database.allRings) {
      val ringOption = JsonObject("ring" -> ring)
      // Checking sources
      for (map <- jgetArray(t, "map")) {
        val from = jgetString(map, "from")
        val prefix = jgetString(map, "toprefix")
        val (toOption, toName) = if (prefix == "") {
          (ringOption, table.tableName)
        } else {
          (ringOption + ("prefixtab" -> prefix), table.tableName + ":" + prefix)
        }
        println(from + "=>" + toName + " on ring " + ring)
        val mapper = mappers(prefix)(from).asInstanceOf[MapReduce.MapAll]
        val mapperInfo = MapperInfo(table, mapper, map, prefixes)
        for (kv <- table.database.table(from).all(ringOption + ("get" -> "kv"))) {
          val k = jget(kv, "k")
          val v = jget(kv, "v")
          for ((k1, v1) <- mapperInfo.mapTo(k, v)) {
            table.get(k1, toOption) match {
              case Some(v2) => {
                //if (prefix != "") println(Compact(k1) + ":" + Compact(v1) + "==" + Compact(v2))
                if (true || Compact(v1) != Compact(v2)) {
                  println("Wrong destination item value " + loc(toName, k1, v1) + " from " + mapperInfo.fromLoc(k, v) + " on ring " + ring)
                  // TODO fix
                }
              }
              case None => {
                println("Missing destination item " + loc(toName, k1) + " from " + mapperInfo.fromLoc(k, v) + " on ring " + ring)
                // TODO fix
              }
            }
            val k2 = mapper.from(k1)
            if (Compact(k2) != Compact(k)) {
              println("Bad map inverse " + Compact(k2) + " from " + mapperInfo.fromLoc(k, v) + " via " + Compact(k1) + " on ring " + ring)
              // TODO can't fix: user map class error
            }
          }
        }
      }
      // Checking destination
      println("DEST")
      mappers.get("") match {
        case Some(mapper) => {
          for (kvc <- table.all(ringOption + ("get" -> "kvc"))) {
            checkMapInverse(table, mapper, ringOption, ring, table.tableName, kvc)
          }
        }
        case _ =>
      }
      // Checking prefix destinations
      for (pInfo <- p) {
        val prefix = jgetString(pInfo, "name")
        println("DEST:" + prefix)
        for (kvc <- table.all(ringOption + ("prefixtab" -> prefix, "get" -> "kvc"))) {
          checkMapInverse(table, mappers(prefix), ringOption, ring, table.tableName + ":" + prefix, kvc)
        }
      }
    }
  }

  private def checkMapInverse(table: Table, mappers: Map[String, MapReduce.MapAll], ringOption: JsonObject, ring: String, toName: String, kvc: Json) {
    val k = jget(kvc, "k")
    val v = jget(kvc, "v")
    val c = jget(kvc, "c")
    val from = jgetObject(c).keys.head
    // TODO act2 nyi
    mappers(from) match {
      case mapper: MapReduce.Map => {
        val k1 = mapper.from(k)
        val v1 = table.database.table(from).get(k1, ringOption).getOrElse(null)
        println("INVERSE:" + Compact(k) + "->" + Compact(k1))
        if (!mapper.to(k1, v1).exists { case (k2, v2) => k2 == k }) {
          println("Extra destination item " + loc(table.tableName, k, v) + " from " + loc(from, k1, v1) + " on ring " + ring)
          // TODO fix
        }
      }
      case _ => // act2 NYI
    }
  }
  */

  private def checkReduces(table: Table, t: Json) {
    val ring0 = table.database.allRings.head
    for (ring <- table.database.allRings) {
      for (reduce <- jgetArray(t, "reduce")) {
        // TODO add reduce checks
        if (ring == ring0) println("    REDUCE NYI")
      }
    }
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
      checkMap.checkMaps1(database, table, t, p)
      checkReduces(table, t)
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
