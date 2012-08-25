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
import akka.actor.ActorRef
import MapReduce._
import scala.collection.immutable.HashMap
import Codes.emptyResponse

private[persist] trait ServerTableMapReduceComponent { this: ServerTableAssembly =>
  val mr: ServerTableMapReduce
  class ServerTableMapReduce {

    case class MapInfo(val to: String, val map: MapAll)
    case class ReduceInfo(val to: String, val reduce: Reduce, val reduceStore: StoreTable)
    
    case class PrefixInfo(val size:Int,val store:StoreTable,var sentMeta:String = "",var sentKey:String = "")

    var maps: List[MapInfo] = Nil
    var reduces: List[ReduceInfo] = Nil
    var prefixes = new HashMap[String, PrefixInfo]()

    val tconfig = info.config.tables(info.tableName)
    val hasMap = tconfig.fromMap.size > 0
    val hasReduce = tconfig.fromReduce.size > 0

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
    for ((to, reduce) <- tconfig.fromReduce) {
      val rtname = info.tableName + "@" + to
      val reduceStore = info.store.getTable(rtname)
      val ri = ReduceInfo(to, getReduce(reduce), reduceStore)
      reduces = ri :: reduces
    }
    var fromReduce: Reduce = null
    for ((from,reduce) <- tconfig.toReduce) {
      ops.canWrite = false
      fromReduce = getReduce(reduce)
    }

    def close {
      for (ri <- reduces) {
        ri.reduceStore.close()
      }
      for ((name, info) <- prefixes) {
        info.store.close()
      }
    }
        
    def delete {
      for (ri <- reduces) {
        ri.reduceStore.delete()
      }
      for ((name, info) <- prefixes) {
        info.store.delete()
      }
    }
    
    def ackPrefix(prefix:String,key:String,meta:String) {
      prefixes.get(prefix) match {
        case Some(pinfo:PrefixInfo) => {
          val oldC = jget(Json(pinfo.sentMeta),"c")
          val newC =jget(Json(meta),"c")
          if (pinfo.sentKey == key && ClockVector.compare(oldC,newC) == '>') {
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
      //info.send ! ("map", dest, ret, to, key, (prefix, passMeta, value))
      info.send ! ("map", info.ringName, to, key, (prefix, passMeta, value))
    }

    private def deletePair(to: String, prefix: String, key: String, value: String) {
      //val t = System.currentTimeMillis()
      val t = info.uidGen.get
      val dest = Map("ring" -> info.ringName)
      val ret = "" // TODO will eventually hook this up
      val passMeta = Compact(JsonObject("c" -> JsonObject(info.tableName -> t), "d" -> true))
      //info.send ! ("mapd", dest, ret, mi.to, oldK, "")
      //info.send ! ("map", dest, ret, to, key, (prefix, passMeta, "null"))
      info.send ! ("map", info.ringName, to, key, (prefix, passMeta, "null"))
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
                  val premeta = pinfo.store.getMeta(prekey) match {
                    case Some(s) => Json(s)
                    case None => info.absentMetaS
                  }
                  if (jgetBoolean(premeta, "d")) {
                    None
                  } else {
                    pinfo.store.get(prekey) match {
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
      val joldValue = Json(oldValue)
      val jvalue = Json(value)
      val hasOld = !jgetBoolean(Json(oldMeta), "d")
      val hasNew = !jgetBoolean(Json(meta), "d")
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
      val jkey = keyDecode(key)
      val jvalue = Json(value)
      val joldvalue = Json(oldvalue)
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

    def map(key: String, prefix: String, meta: String, value: String): (String, Any) = {
      val (size, store,pinfo) = if (prefix == "") {
        (0, info.storeTable,null)
      } else {
        prefixes.get(prefix) match {
          case Some(pinfo) => (pinfo.size, pinfo.store,pinfo)
          case None => (0, info.storeTable,null)
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
        //val oldvS = info.storeTable.get(key) match {
        val oldvS = store.get(key) match {
          case Some(s: String) => s
          case None => "null"
        }
        store.putBoth(key, meta, value)
        if (prefix == "") {
          doMR(key, oldMetaS, oldvS, meta, value)
        } else {
          if (bal.inNext(key)) {
            pinfo.sentKey = key
            pinfo.sentMeta = meta
            bal.sendPrefix(prefix,key,meta,value)
          }
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
                    case None => "null"
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

    def reduceOut(key: String, oldMeta: String, oldValue: String, meta: String, value: String) {
      // TODO handle balance
      val jkey = keyDecode(key)
      val jkeya = jkey match {
        case j: JsonArray => j
        case _ => return // key is not an array
      }
      // jkey must be an array
      val joldValue = Json(oldValue)
      val jvalue = Json(value)
      val hasOld = !jgetBoolean(Json(oldMeta), "d")
      val hasNew = !jgetBoolean(Json(meta), "d")
      for (ri <- reduces) {
        if (jkeya.size < ri.reduce.size) return // key is too short
        var prefix = JsonArray()
        var i = 0
        for (elem <- jkeya) {
          if (i < ri.reduce.size) {
            prefix = elem +: prefix
          }
          i += 1
        }
        prefix = prefix.reverse
        val oldsum = ri.reduceStore.get(keyEncode(prefix)) match {
          case Some(s: String) => Json(s)
          case None => ri.reduce.zero
        }
        var newsum = oldsum

        val (oldMapKey, oldMapVal) = if (hasOld) {
          (jkey, joldValue)
        } else {
          (null, null)
        }
        val (newMapKey, newMapVal) = if (hasNew) {
          (jkey, jvalue)
        } else {
          (null, null)
        }

        // do local reduction
        if (oldMapVal == null && newMapVal == null) return
        if (oldMapVal != null) newsum = ri.reduce.subtract(newsum, ri.reduce.item(jkey, oldMapVal))
        if (newMapVal != null) newsum = ri.reduce.add(newsum, ri.reduce.item(jkey, newMapVal))

        // save and send to dest
        if (Compact(oldsum) != Compact(newsum)) {
          val newsum1 = if (Compact(newsum) == Compact(ri.reduce.zero)) {
            ri.reduceStore.remove(keyEncode(prefix))
            ri.reduce.zero
          } else {
            ri.reduceStore.put(keyEncode(prefix), Compact(newsum))
            newsum
          }
          val dest = Map("ring" -> info.ringName)
          val ret = "" // TODO will eventually hook this up
          //val t = System.currentTimeMillis()
          val t = info.uidGen.get
          //info.send ! ("reduce", dest, ret, ri.to, keyEncode(prefix), (info.nodeName, Compact(newsum1), t))
          info.send ! ("reduce", info.ringName, ri.to, keyEncode(prefix), (info.nodeName, Compact(newsum1), t))
        }
      }
    }

    def reduce(key: String, node: String, item: String, newt: Long): (String, Any) = {
      // {"c":{"node":t},"r":{"node":"sum"}}
      var oldMetaS = info.storeTable.getMeta(key) match {
        case Some(s: String) => s
        case None => info.absentMetaS
      }
      var meta = jgetObject(Json(oldMetaS))
      val c = jgetObject(meta, "c")
      val oldt = jgetLong(c, node)
      if (newt > oldt) {
        meta = meta + ("c" -> (c + (node -> newt)))
        var items = jgetObject(meta, "r")
        if (item == Compact(fromReduce.zero)) {
          items = items - node
        } else {
          items = items + (node -> Json(item))
        }
        if (items.keySet.size == 0) {
          meta = meta + ("d" -> true)
        } else {
          meta = meta - "d"
        }
        var sum = fromReduce.zero
        for ((name, itemval) <- items) {
          sum = fromReduce.add(sum, itemval)
        }
        meta = meta + ("r" -> items)
        val cmeta = Compact(meta)
        val csum = Compact(sum)
        val oldSum = info.storeTable.get(key) match {
          case Some(s) => s
          case None => "null"
        }
        info.storeTable.putBoth(key, cmeta, csum)
        doMR(key, oldMetaS, oldSum, cmeta, csum)
      }
      (Codes.Ok, emptyResponse)
    }

    def doMR(key: String, oldMeta: String, oldValue: String, meta: String, value: String) {
      if (hasMap) mr.mapOut(key, oldMeta, oldValue, meta, value)
      if (hasReduce) mr.reduceOut(key, oldMeta, oldValue, meta, value)
    }
  }
}