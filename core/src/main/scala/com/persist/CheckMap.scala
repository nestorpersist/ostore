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

private[persist] class CheckMap(client:Client, cmd: String, config: Json) {

  private val now = System.currentTimeMillis()
  //private val client = new Client(jget(config, "client"))
  val fix = cmd == "fix"

  private def loc(table: String, key: JsonKey) = table + ":" + Compact(key)
  private def loc(table: String, key: JsonKey, value: Json) = table + ":(" + Compact(key) + "," + Compact(value) + ")"
  private def loc(table: String, pkey: JsonKey, pvalue: Json, key: JsonKey, value: Json) =
    table + ":(" + Compact(pkey) + "," + Compact(pvalue) + ")" +
      ":(" + Compact(key) + "," + Compact(value) + ")"

  private class PrefixVals(prefix: String, table: Table, size: Int, fromOptions: JsonObject) {
    val opt = fromOptions + ("prefixtab" -> prefix)
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
    val ring: String
    val toName: String
    val toOption: JsonObject
    val fromName: String
    val fromOption: JsonObject
    val fromTable: Table
    def mapTo(k: JsonKey, v: Json): Traversable[(JsonKey, Json)]
    def mapFrom(k: JsonKey): JsonKey
    def fromLoc(k: JsonKey, v: Json): String
  }

  private object MapperInfo {
    def apply(table: Table, mapper: MapReduce.MapAll, map: Json, prefixes: Map[String, Int], ring: String): MapperInfo = {
      val toPrefix = jgetString(map, "toprefix")
      val (toName, toOption) = {
        if (toPrefix == "") {
          (table.tableName, JsonObject("ring" -> ring))
        } else {
          (table.tableName + ":" + toPrefix, JsonObject("ring" -> ring, "prefixtab" -> toPrefix))
        }
      }
      if (jgetString(map, "act2") != "") {
        new MapperInfo2(ring, toName, toOption, table, mapper, map, prefixes)
      } else {
        new MapperInfo1(ring, toName, toOption, table, mapper, map)
      }
    }
  }

  private class MapperInfo2(val ring: String, val toName: String, val toOption: JsonObject, table: Table, mapper: MapReduce.MapAll, map: Json, prefixes: Map[String, Int]) extends MapperInfo {
    private val map2 = mapper.asInstanceOf[MapReduce.Map2]
    private val fromPrefix = map2.fromprefix
    val fromName = jgetString(map, "from")
    val fromOption = JsonObject("ring" -> ring)
    val fromTable = table.database.table(fromName)
    private val size = prefixes(fromPrefix)
    private val pvals = new PrefixVals(fromPrefix, table, size, fromOption)
    def mapTo(k: JsonKey, v: Json) = {
      val (pk, opv) = pvals.get(k)
      opv match {
        case Some(pv: Json) => {
          val m2 = map2.to(pk, pv, k, v)
          m2
        }
        case None => List()
      }
    }
    def mapFrom(k: JsonKey) = map2.from(k)
    def fromLoc(k: JsonKey, v: Json) = {
      val (pk, opv) = pvals.get(k)
      loc(fromPrefix, pk, opv.getOrElse(null), k, v)
    }
  }

  private class MapperInfo1(val ring: String, val toName: String, val toOption: JsonObject, table: Table, mapper: MapReduce.MapAll, map: Json) extends MapperInfo {
    val fromName = jgetString(map, "from")
    val fromOption = JsonObject("ring" -> ring)
    val fromTable = table.database.table(fromName)
    private val map1 = mapper.asInstanceOf[MapReduce.Map]
    def mapTo(k: JsonKey, v: Json) = map1.to(k, v)
    def mapFrom(k: JsonKey) = map1.from(k)
    def fromLoc(k: JsonKey, v: Json) = loc(fromName, k, v)
  }

  private def getAllMappers(t: Json) = {
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

  private def getAllMappers1(table: Table, t: Json, p: JsonArray, ring: String): Map[String, Map[String, MapperInfo]] = {
    val prefixes = p.map(i => (jgetString(i, "name") -> jgetInt(i, "size"))).toMap

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

    def getMapperInfo(m: Json) = {
      val mapper = getMapper(m)
      MapperInfo(table, mapper, m, prefixes, ring)
    }

    // Map[(toprefix,from),mapperInfo]
    val mappers1 = jgetArray(t, "map").map(m => ((jgetString(m, "toprefix"), jgetString(m, "from")), getMapperInfo(m))).toMap

    // Map[toprefix,Map[(toprefix,from),mapperInfo]]
    val mappers2 = mappers1.groupBy { case ((prefix, from), m1) => prefix }

    // Map[toprefix,Map[from,mapperInfo]]
    val mappers = mappers2.map { case (prefix, m1) => prefix -> m1.map { case ((prefix, from), map) => from -> map } }
    mappers
  }

  def checkForward(table: Table, mapperInfo: MapperInfo) {
    println(mapperInfo.fromName + "=>" + mapperInfo.toName + " on ring " + mapperInfo.ring)
    for (kv <- mapperInfo.fromTable.all(mapperInfo.fromOption + ("get" -> "kv"))) {
      val k = jget(kv, "k")
      val v = jget(kv, "v")
      for ((k1, v1) <- mapperInfo.mapTo(k, v)) {
        table.get(k1, mapperInfo.toOption) match {
          case Some(v2) => {
            if (Compact(v1) != Compact(v2)) {
              println("Wrong destination item value " + loc(mapperInfo.toName, k1, v1) + " from " + mapperInfo.fromLoc(k, v) + " on ring " + mapperInfo.ring)
              // TODO fix
            }
          }
          case None => {
            println("Missing destination item " + loc(mapperInfo.toName, k1) + " from " + mapperInfo.fromLoc(k, v) + " on ring " + mapperInfo.ring)
            // TODO fix
          }
        }
        val k2 = mapperInfo.mapFrom(k1)
        if (Compact(k2) != Compact(k)) {
          println("Bad map inverse " + Compact(k2) + " from " + mapperInfo.fromLoc(k, v) + " via " + Compact(k1) + " on ring " + mapperInfo.ring)
          // TODO can't fix: user map class error
        }
      }
    }
  }

  def checkBackward(table: Table, prefix: String, mappers: Map[String, MapperInfo], ring: String) {
    println("Inverse of " + table.tableName + ":" + prefix + " on ring " + ring)
    for (kvc <- table.all(JsonObject("ring" -> ring, "prefixtab" -> prefix, "get" -> "kvc"))) {
      val k = jget(kvc, "k")
      val v = jget(kvc, "v")
      val c = jget(kvc, "c")
      val from = jgetObject(c).keys.head
      val mapperInfo = mappers(from)
      val k1 = mapperInfo.mapFrom(k)
      val v1 = mapperInfo.fromTable.get(k1, mapperInfo.fromOption).getOrElse(null)
      if (!mapperInfo.mapTo(k1, v1).exists { case (k2, v2) => k2 == k }) {
        println("Extra destination item " + loc(mapperInfo.toName, k, v) + " from " + loc(mapperInfo.fromName, k1, v1) + " on ring " + ring)
        // TODO fix
      }
    }
  }

  private def checkMaps1(database: Database, table: Table, t: Json, p: JsonArray, ring: String, ringOption: JsonObject) {
    val mappers = getAllMappers1(table, t, p, ring)
    for ((prefix, m1) <- mappers) {
      for ((from, mapInfo) <- m1) {
        checkForward(table, mapInfo)
      }
    }
    for ((prefix, m1) <- mappers) {
      checkBackward(table, prefix, m1, ring)
      // bwd (main, prefix)
    }
  }

  def checkMaps(database: Database, table: Table, t: Json, p: JsonArray) {
    for (ring <- table.database.allRings) {
      val ringOption = JsonObject("ring" -> ring)
      checkMaps1(database, table, t, p, ring, ringOption)
    }
  }

}
