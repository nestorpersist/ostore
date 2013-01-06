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

import JsonOps._
import JsonKeys._
import akka.actor.ActorRef
import MapReduce._
import scala.collection.immutable.HashMap
import Codes.emptyResponse
import Stores._

private[persist] trait ServerTableMapComponent { this: ServerTableAssembly =>
  val map: ServerTableMap
  class ServerTableMap {

    case class MapInfo(val to: String, val map: MapAll)

    case class PrefixInfo(val size: Int, val storeTable: StoreTable, var sentMeta: String = "", var sentKey: String = "")

    var maps: List[MapInfo] = Nil
    var prefixes = new HashMap[String, PrefixInfo]()

    val tconfig = info.config.tables(info.tableName)
    val hasMap = tconfig.fromMap.size > 0

    for (p <- tconfig.prefix) {
      val name = jgetString(p, "name")
      val size = jgetInt(p, "size")
      val pname = info.tableName + ":" + name
      prefixes = prefixes + (name -> PrefixInfo(size, info.store.getTable(pname)))
    }

    for ((to, map) <- tconfig.fromMap) {
      val mi = MapInfo(to, getMap(map))
      maps = mi :: maps
    }
    if (tconfig.toMap.keySet.size > 0) {
      ops.canWrite = false
    }

    def close {
      for ((name, info) <- prefixes) {
        info.storeTable.close()
      }
    }

    def delete {
      for ((name, info) <- prefixes) {
        info.storeTable.store.deleteTable(info.storeTable.tableName)
      }
    }

    def ackPrefix(prefix: String, key: String, meta: String) {
      prefixes.get(prefix) match {
        case Some(pinfo: PrefixInfo) => {
          val oldC = jget(Json(pinfo.sentMeta), "c")
          val newC = jget(Json(meta), "c")
          if (pinfo.sentKey == key && ClockVector.compare(oldC, newC) == '>') {
            pinfo.sentMeta = ""
            pinfo.sentKey = ""
          }
        }
        case None =>
      }
    }

    private def putPair(to: String, prefix: String, key: String, value: String) {
      //val t = System.currentTimeMillis()
      val t = info.uidGen.get
      val dest = Map("ring" -> info.ringName)
      val ret = "" // TODO will eventually hook this up
      val passMeta = Compact(JsonObject("c" -> JsonObject(info.tableName -> t)))
      info.send ! ("map", info.ringName, 0L, to, key, (emptyResponse, prefix, passMeta, value))
    }

    private def deletePair(to: String, prefix: String, key: String, value: String) {
      //val t = System.currentTimeMillis()
      val t = info.uidGen.get
      val dest = Map("ring" -> info.ringName)
      val ret = "" // TODO will eventually hook this up
      val passMeta = Compact(JsonObject("c" -> JsonObject(info.tableName -> t), "d" -> true))
      info.send ! ("map", info.ringName, 0L, to, key, (emptyResponse, prefix, passMeta, NOVAL))
    }

    private def mapPairs(to: String, prefix: String, dedup: Boolean, hasOld: Boolean, hasNew: Boolean,
      oldPairs: Traversable[(JsonKey, Json)], newPairs: Traversable[(JsonKey, Json)]) {
      if (dedup || (hasNew && hasOld)) {
        var old = Map[String, String]()
        var newKeys = Set[String]()
        for (pair <- oldPairs) {
          val (key, value) = pair
          old = old + (keyEncode(key) -> Compact(value))
        }
        for (pair <- newPairs) {
          val (key, value) = pair
          val key1 = keyEncode(key)
          val value1 = Compact(value)
          val dup = if (dedup) {
            if (newKeys.contains(key1)) {
              true
            } else {
              newKeys = newKeys + key1
              false
            }
          } else {
            false
          }
          if (dup) {
          } else if (old.contains(key1)) {
            val oldValue = old.get(key1) match {
              case Some(s: String) => s
              case None => ""
            }
            if (oldValue != value1) {
              putPair(to, prefix, key1, value1)
            }
            old = old - key1
          } else {
            putPair(to, prefix, key1, value1)
          }
        }
        for (key <- old.keys) {
          val value = old.get(key) match {
            case Some(s: String) => s
            case None => ""
          }
          deletePair(to, prefix, key, value)
        }
      } else if (hasOld) {
        for (pair <- oldPairs) {
          val (key, value) = pair
          deletePair(to, prefix, keyEncode(key), Compact(value))
        }
      } else if (hasNew) {
        for (pair <- newPairs) {
          val (key, value) = pair
          putPair(to, prefix, keyEncode(key), Compact(value))
        }
      }
    }

    private def getPrevalue(jkey: Json, prefix: String): Option[(JsonKey, Json)] = {
      if (prefix == "") {
        None
      } else {
        prefixes.get(prefix) match {
          case Some(pinfo) => {
            jkey match {
              case a: JsonArray => {
                if (a.size >= pinfo.size) {
                  val jprekey = a.take(pinfo.size)
                  val prekey = keyEncode(jprekey)
                  val premeta = pinfo.storeTable.getMeta(prekey) match {
                    case Some(s) => Json(s)
                    case None => info.absentMetaS
                  }
                  if (jgetBoolean(premeta, "d")) {
                    None
                  } else {
                    pinfo.storeTable.get(prekey) match {
                      case Some(s: String) => {
                        Some(jprekey, Json(s))
                      }
                      case None => {
                        None
                      }
                    }
                  }
                } else {
                  None
                }
              }
              case x => {
                None
              }
            }
          }
          case x => {
            None
          }
        }
      }
    }

    def mapOut(key: String, oldMeta: String, oldValue: String, meta: String, value: String) {
      val jkey = keyDecode(key)
      val hasOld = !jgetBoolean(Json(oldMeta), "d")
      val joldValue = if (hasOld) Json(oldValue) else NOVAL
      val hasNew = !jgetBoolean(Json(meta), "d")
      val jvalue = if (hasNew) Json(value) else NOVAL
      for (mi <- maps) {
        val dedup = jgetBoolean(mi.map.options, "dedup")
        val toprefix = jgetString(mi.map.options, "toprefix")
        val fromprefix = jgetString(mi.map.options, "fromprefix")
        val prePair = getPrevalue(jkey, fromprefix)
        val oldPairs = if (hasOld) {
          mi.map match {
            case m: Map => m.to(jkey, joldValue)
            case m: Map2 => {
              prePair match {
                case Some((prekey: JsonKey, prevalue: Json)) => {
                  m.to(prekey, prevalue, jkey, joldValue)
                }
                case None => List[(JsonKey, Json)]()
              }
            }
          }
        } else {
          List[(JsonKey, Json)]()
        }
        val newPairs = if (hasNew) {
          mi.map match {
            case m: Map => m.to(jkey, jvalue)
            case m: Map2 => {
              prePair match {
                case Some((prekey: JsonKey, prevalue: Json)) => {
                  m.to(prekey, prevalue, jkey, jvalue)
                }
                case None => List[(JsonKey, Json)]()
              }
            }
          }
        } else {
          List[(JsonKey, Json)]()
        }
        mapPairs(mi.to, toprefix, dedup, hasOld, hasNew, oldPairs, newPairs)
      }
    }

    private def maps2(prefix: String, key: String, hasOld: Boolean, hasNew: Boolean, value: String, oldvalue: String,
      itemKey: String, itemValue: String) {
      // process an updated prefix table item and a main table item it prefixes
      // passed in 1. prefix table item: old and new prefix values
      //           2. main table item: key and value  
      // Sends updates to downstream tables with a map2 from this prefix
      val jkey = keyDecode(key)
      val jvalue = Json(value)
      val joldvalue = if (hasOld) Json(oldvalue) else NOVAL
      val jitemKey = keyDecode(itemKey)
      val jitemValue = Json(itemValue)
      for (mi <- maps) {
        mi.map match {
          case m: Map2 => {
            if (m.fromprefix == prefix) {
              val dedup = jgetBoolean(m.options, "dedup")
              val toprefix = jgetString(m.options, "toprefix")
              val oldPairs = if (hasOld) {
                m.to(jkey, joldvalue, jitemKey, jitemValue)
              } else {
                List[(JsonKey, Json)]()
              }
              val newPairs = if (hasNew) {
                m.to(jkey, jvalue, jitemKey, jitemValue)
              } else {
                List[(JsonKey, Json)]()
              }
              mapPairs(mi.to, toprefix, dedup, hasOld, hasNew, oldPairs, newPairs)
            }
          }
          case x =>
        }
      }
    }

    def map(key: String, prefix: String, meta: String, value: String, os:String): (String, Any) = {
      val (size, store, pinfo) = if (prefix == "") {
        (0, info.storeTable, null)
      } else {
        prefixes.get(prefix) match {
          case Some(pinfo) => (pinfo.size, pinfo.storeTable, pinfo)
          case None => (0, info.storeTable, null)
        }
      }
      val jmeta = Json(meta)
      val c = jgetObject(jmeta, "c")
      var fromTab = ""
      var newt: Long = 0
      for ((n, t) <- c) {
        fromTab = n
        newt = jgetLong(t)
      }
      val oldMetaS = store.getMeta(key) match {
        case Some(s: String) => s
        case None => info.absentMetaS
      }
      val oldMeta = Json(oldMetaS)
      val oldt = jgetLong(oldMeta, "c", fromTab)
      if (newt > oldt) {
        val oldvS = store.get(key) match {
          case Some(s: String) => s
          case None => NOVAL
        }
        store.put(key, meta, value, true)
        if (prefix == "") {
          // Main table update 
          doMap(key, oldMetaS, oldvS, meta, value)
          reduce.doReduce(key, oldMetaS, oldvS, meta, value)
        } else {
          // Prefix table update
          if (bal.inNext(key)) {
            // Send prefix to next node if needed there
            pinfo.sentKey = key
            pinfo.sentMeta = meta
            bal.sendPrefix(prefix, key, meta, value)
          }
          // Iterate over all items that have this prefix
          var eq = true
          var k = key
          var done = false
          val limit = k + "\uFFFF"
          val hasOld = !jgetBoolean(oldMeta, "d")
          val hasNew = !jgetBoolean(jmeta, "d")
          while (!done) {
            ops.getNext(k, eq, false, emptyJsonObject) match {
              case Some((itemKey: String, k1: String)) => {
                if (itemKey < limit) {
                  val itemValue = info.storeTable.get(itemKey) match {
                    case Some(s: String) => s
                    case None => NOVAL
                  }
                  maps2(prefix, k, hasOld, hasNew, value, oldvS, itemKey, itemValue)
                  k = itemKey
                  eq = false
                } else {
                  done = true
                }

              }
              case None => done = true
            }
          }
        }
      }
      (Codes.Ok, emptyResponse)
    }

    def doMap(key: String, oldMeta: String, oldValue: String, meta: String, value: String) {
      if (hasMap) mapOut(key, oldMeta, oldValue, meta, value)
    }

  }
}