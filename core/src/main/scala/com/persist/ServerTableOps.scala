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
import scala.math.max
import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.util.Timeout
import akka.util.duration._
import akka.actor.ActorSystem
import Exceptions._
import ExceptionOps._
import Codes.emptyResponse

private[persist] trait ServerTableOpsComponent { this: ServerTableAssembly =>
  val ops: ServerTableOps
  class ServerTableOps(system: ActorSystem) {

    lazy implicit private val ec = ExecutionContext.defaultExecutionContext(system)
    implicit private val timeout = Timeout(5 seconds)

    var acceptUserMessages = false
    var canWrite = true

    // Monitor counters
    var cntMsg: Long = 0
    var cntGet: Long = 0
    var timeGet: Long = 0
    var cntPut: Long = 0
    var timePut: Long = 0

    private def getItems(key: String, meta: Json, value: String, items: String): String = {
      if (items != "") {
        var result = JsonObject()
        if (items.contains("k")) result = result + ("k" -> keyDecode(key))
        if (value != Stores.NOVAL && items.contains("v")) result = result + ("v" -> Json(value))
        if (items.contains("c")) result = result + ("c" -> jget(meta, "c"))
        if (items.contains("d")) result = result + ("d" -> jgetBoolean(meta, "d"))
        if (items.contains("e")) result = result + ("e" -> jgetLong(meta, "e"))
        Compact(result)
      } else {
        value
      }
    }

    private def inLow(k: String, less: Boolean): Boolean = {
      if (bal.singleNode) {
        true
      } else {
        if (less) {
          k <= info.high
        } else {
          k < info.high
        }
      }
    }

    private def inHigh(k: String, less: Boolean): Boolean = {
      if (bal.singleNode) {
        false
      } else if (info.low > info.high) {
        if (less) {
          k > info.low
        } else {
          k >= info.low
        }
      } else {
        false
      }
    }

    private def get1(key: String, os: String): (String, Any) = {
      val options = Json(os)
      val prefixtab = jgetString(options, "prefixtab")
      val store = if (prefixtab == "") {
        info.storeTable
      } else {
        map.prefixes.get(prefixtab) match {
          case Some(pinfo) => pinfo.storeTable
          case None => return (Codes.NoTable, Compact(JsonObject("table" -> info.tableName, "prefixtab" -> prefixtab)))
        }
      }
      val get = jgetString(options, "get")
      val meta = store.getMeta(key)
      meta match {
        case Some(ms) => {
          val m = Json(ms)
          if (jgetBoolean(m, "d") && !get.contains("d")) {
            (Codes.NoItem, emptyResponse)
          } else {
            val value = store.get(key)
            value match {
              case Some(vs) => {
                val result = getItems(key, m, vs, get)
                (Codes.Ok, result)
              }
              case None => (Codes.InternalError, "internal:meta-v sync error")
            }
          }
        }
        case None => (Codes.NoItem, emptyResponse)
      }
    }

    def get(key: String, os: String): (String, Any) = {
      val t0 = System.currentTimeMillis()
      cntGet += 1
      val result = get1(key, os)
      val t1 = System.currentTimeMillis()
      val delta = max(t1 - t0, 1)
      timeGet += delta
      info.monitor ! ("reportget", info.tableName, cntGet, timeGet)
      result
    }

    private def put1(key: String, value: String, requests: String): (String, Any) = {
      val request = Json(requests)
      val fast = jgetBoolean(request, "o", "fast")
      val expires = jgetLong(request, "o", "expires")
      val doSync = !jgetBoolean(request, "o", "testnosync")
      val oldMetaS = info.storeTable.getMeta(key) match {
        case Some(s: String) => s
        case None => info.absentMetaS
      }
      val oldMeta = Json(oldMetaS)
      val oldD = jgetBoolean(oldMeta, "d")
      if (!oldD && jgetBoolean(request, "create")) {
        // Create and already exists
        return (Codes.Conflict, emptyResponse)
      }
      val oldvS = if (oldD) {
        Stores.NOVAL
      } else {
        info.storeTable.get(key) match {
          case Some(s: String) => s
          case None => Stores.NOVAL
        }
      }
      val oldcv = jget(oldMeta, "c")
      val requestcv = jget(request, "c")
      if (requestcv != null) {
        if (ClockVector.compare(oldcv, requestcv) != '=') {
          // Opt put and value has changed
          return (Codes.Conflict, emptyResponse)
        }
      }
      val d = info.uidGen.get
      val cv = ClockVector.incr(oldcv, info.ringName, d)
      var newMeta = JsonObject("c" -> cv)
      if (expires != 0) newMeta += ("e" -> expires)
      val newMetaS = Compact(newMeta)
      info.storeTable.put(key, newMetaS, value, fast)
      if (doSync && sync.hasSync) sync.toRings(key, oldMetaS, oldvS, newMetaS, value)
      map.doMap(key, oldMetaS, oldvS, newMetaS, value)
      reduce.doReduce(key, oldMetaS, oldvS, newMetaS, value)
      (Codes.Ok, emptyResponse)
    }

    def put(key: String, value: String, requests: String): (String, Any) = {
      cntPut += 1
      val t0 = System.currentTimeMillis()
      val result = put1(key, value, requests)
      val t1 = System.currentTimeMillis()
      val delta = max(t1 - t0, 1)
      timePut += delta
      info.monitor ! ("reportput", info.tableName, cntPut, timePut)
      result
    }

    def delete(key: String, requests: String): (String, Any) = {
      val request = Json(requests)
      val fast = jgetBoolean(request, "o", "fast")
      val doSync = !jgetBoolean("request", "o", "testnosync")
      val value = Stores.NOVAL
      val oldMetaS = info.storeTable.getMeta(key) match {
        case Some(s: String) => s
        case None => info.absentMetaS
      }
      val oldvS = info.storeTable.get(key) match {
        case Some(s: String) => s
        case None => Stores.NOVAL
      }
      val oldcv = jget(Json(oldMetaS), "c")
      val requestcv = jget(request, "c")
      if (requestcv != null) {
        if (ClockVector.compare(oldcv, requestcv) != '=') {
          // Opt delete and value has changed
          return (Codes.Conflict, emptyResponse)
        }
      }
      val d = info.uidGen.get
      val cv = ClockVector.incr(oldcv, info.ringName, d)
      val newMeta = JsonObject("c" -> cv, "d" -> true)
      val newMetaS = Compact(newMeta)
      // if fast, commit will run in background
      info.storeTable.put(key, newMetaS, value, fast)
      if (doSync && sync.hasSync) sync.toRings(key, oldMetaS, oldvS, newMetaS, value)
      map.doMap(key, oldMetaS, oldvS, newMetaS, value)
      reduce.doReduce(key, oldMetaS, oldvS, newMetaS, value)
      (Codes.Ok, emptyResponse)
    }

    def resync(key: String, requests: String): (String, Any) = {
      if (sync.hasSync) {
        val metaS = info.storeTable.getMeta(key) match {
          case Some(s: String) => s
          case None => info.absentMetaS
        }
        val value = info.storeTable.get(key) match {
          case Some(s: String) => s
          case None => Stores.NOVAL
        }
        sync.toRings(key, info.absentMetaS, Stores.NOVAL, metaS, value)
      }
      (Codes.Ok, emptyResponse)
    }

    def next(kind: String, key: String, os: String): (String, Any) = {
      val options = Json(os)
      val get = jgetString(options, "get")
      val equal = kind == "next"
      val key1 = getNext(key, equal, get.contains("d"), options)
      key1 match {
        case Some((key2: String, result: String)) => {
          if (inLow(key, false) && inLow(key2, false)) {
            (Codes.Ok, result)
          } else if (inHigh(key, false) && inHigh(key2, false)) {
            (Codes.Ok, result)
          } else {
            (Codes.Next, info.high)
          }
        }
        case None => {
          if (inLow(key, false) && !bal.singleNode) {
            (Codes.Next, info.high)
          } else {
            (Codes.Done, emptyResponse)
          }
        }
      }
    }

    def prev(kind: String, key: String, os: String): (String, Any) = {
      val equal = kind == "prev"
      val less = kind == "prev-"
      val options = Json(os)
      val get = jgetString(options, "get")
      val key1 = getPrev(key, equal, get.contains("d"), options)
      key1 match {
        case Some((key2: String, result: String)) => {
          if (inLow(key, less) && inLow(key2, false)) {
            (Codes.Ok, result)
          } else if (inHigh(key, less) && inHigh(key2, false)) {
            (Codes.Ok, result)
          } else {
            (Codes.PrevM, info.low)
          }
        }
        case None => {
          if (info.low <= info.high && !bal.singleNode) {
            (Codes.PrevM, info.low)
          } else if (inHigh(key, less)) {
            (Codes.PrevM, info.low)
          } else {
            (Codes.Done, emptyResponse)
          }
        }
      }
    }

    def getNext(key: String, equal: Boolean, includeDelete: Boolean, options: Json): Option[(String, String)] = {
      val prefixtab = jgetString(options, "prefixtab")
      val store = if (prefixtab == "") {
        info.storeTable
      } else {
        map.prefixes.get(prefixtab) match {
          case Some(pinfo) => pinfo.storeTable
          case None => return Some((Codes.NoTable, Compact(JsonObject("table" -> info.tableName, "prefixtab" -> prefixtab))))
        }
      }
      val key1 = store.next(key, equal)
      key1 match {
        case Some(key2: String) => {
          store.getMeta(key2) match {
            case Some(metas: String) => {
              val meta = Json(metas)
              if ((!includeDelete) && jgetBoolean(meta, "d")) {
                getNext(key2, false, false, options)
              } else {
                val get = jget(options, "get") match {
                  case null => ""
                  case s: String => s + "k"
                }
                if (get == "") {
                  Some((key2, Compact(keyDecode(key2))))
                } else {
                  val v = if (get.contains("v")) {
                    store.get(key2) match {
                      case Some(s: String) => s
                      case None => Stores.NOVAL
                    }
                  } else {
                     Stores.NOVAL
                  }
                  val result = getItems(key2, meta, v, get)
                  Some((key2, result))
                }
              }
            }
            case None => {
              throw InternalException("getNext")
            }
          }
        }
        case None => None
      }
    }

    def getPrev(key: String, equal: Boolean, includeDelete: Boolean, options: Json): Option[(String, String)] = {
      val prefixtab = jgetString(options, "prefixtab")
      val store = if (prefixtab == "") {
        info.storeTable
      } else {
        map.prefixes.get(prefixtab) match {
          case Some(pinfo) => pinfo.storeTable
          case None => return Some((Codes.NoTable, Compact(JsonObject("table" -> info.tableName, "prefixtab" -> prefixtab))))
        }
      }
      val key1 = store.prev(key, equal)
      key1 match {
        case (Some(key2: String)) => {
          store.getMeta(key2) match {
            case Some(metas: String) => {
              val meta = Json(metas)
              if ((!includeDelete) && jgetBoolean(meta, "d")) {
                getPrev(key2, false, false, options)
              } else {
                val get = jget(options, "get") match {
                  case null => ""
                  case s: String => s + "k"
                }
                if (get == "") {
                  Some((key2, Compact(keyDecode(key2))))
                } else {
                  val v = if (get.contains("v")) {
                    store.get(key2) match {
                      case Some(s: String) => s
                      case None => Stores.NOVAL
                    }
                  } else {
                    Stores.NOVAL
                  }
                  val result = getItems(key2, meta, v, get)
                  Some((key2, result))
                }
              }
            }
            case None => {
              throw InternalException("getPrev")
            }
          }
        }
        case None => None
      }
    }
  }
}