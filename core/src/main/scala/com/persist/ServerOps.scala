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
import scala.Math.max
import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.util.Timeout
import akka.util.duration._
import akka.actor.ActorSystem

trait ServerOpsComponent { this: ServerTableAssembly =>
  val ops: ServerOps
  class ServerOps(system: ActorSystem) {

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
        if (items.contains("v")) result = result + ("v" -> Json(value))
        if (items.contains("c")) result = result + ("c" -> jget(meta, "c"))
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
      val meta = info.storeTable.getMeta(key)
      meta match {
        case Some(ms) => {
          val m = Json(ms)
          if (jgetBoolean(m, "d")) {
            (Codes.NotPresent, "")
          } else {
            val value = info.storeTable.get(key)
            value match {
              case Some(vs) => {
                val get = jgetString(options, "get")
                val result = getItems(key, m, vs, get)
                (Codes.Ok, result)
              }
              case None => (Codes.InternalError, "internal:meta-v sync error")
            }
          }
        }
        case None => (Codes.NotPresent, "")
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
      val oldMetaS = info.storeTable.getMeta(key) match {
        case Some(s: String) => s
        case None => info.absentMetaS
      }
      val oldMeta = Json(oldMetaS)
      val oldD = jgetBoolean(oldMeta, "d")
      if (! oldD && jgetBoolean(request, "create")) {
        // Create and already exists
        return (Codes.NoPut, "null")
      }
      val oldvS = if (oldD) {
        "null"
      } else {
        info.storeTable.get(key) match {
          case Some(s: String) => s
          case None => "null"
        }
      }
      val oldcv = jget(oldMeta, "c")
      val requestcv = jget(request, "c")
      if (requestcv != null) {
        if (ClockVector.compare(oldcv, requestcv) != '=') {
          // Opt put and value has changed
          return (Codes.NoPut, "null")
        }
      }
      ///val d = System.currentTimeMillis()
      val d = info.uidGen.get
      val cv = ClockVector.incr(oldcv, info.ringName, d)
      val newMeta = JsonObject("c" -> cv)
      val newMetaS = Compact(newMeta)
      info.storeTable.putBothF1(key, newMetaS, value, fast)
      if (sync.hasSync) sync.msgOut(key, oldMetaS, oldvS, newMetaS, value)
      if (mr.hasMap) mr.mapOut(key, oldMetaS, oldvS, newMetaS, value)
      if (mr.hasReduce) mr.reduceOut(key, oldMetaS, oldvS, newMetaS, value)
      //mr.doMR(key, oldMetaS, oldvS, newMetaS, value)
      (Codes.Ok, "null")
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
      val value = "null"
      val oldMetaS = info.storeTable.getMeta(key) match {
        case Some(s: String) => s
        case None => info.absentMetaS
      }
      val oldvS = info.storeTable.get(key) match {
        case Some(s: String) => s
        case None => "null"
      }
      val oldcv = jget(Json(oldMetaS), "c")
      //val d = System.currentTimeMillis()
      val d = info.uidGen.get
      val cv = ClockVector.incr(oldcv, info.ringName, d)
      val newMeta = JsonObject("c" -> cv, "d" -> true)
      val newMetaS = Compact(newMeta)
      // if fast, commit will run in background
      //val f = info.storeTable.putBothF(key, newMetaS, value, fast)
      info.storeTable.putBothF1(key, newMetaS, value, fast)
      sync.msgOut(key, oldMetaS, oldvS, newMetaS, value)
      mr.doMR(key, oldMetaS, oldvS, newMetaS, value)
      //f map { x =>
        (Codes.Ok, "null")
      //}
    }

    def next(kind: String, key: String, os: String): (String, Any) = {
      val options = Json(os)
      val equal = kind == "next"
      val key1 = getNext(key, equal, false, options)
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
            (Codes.Done, "")
          }
        }
      }
    }

    def prev(kind: String, key: String, os: String): (String, Any) = {
      val equal = kind == "prev"
      val less = kind == "prev-"
      val options = Json(os)
      val key1 = getPrev(key, equal, false, options)
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
            (Codes.Done, "")
          }
        }
      }
    }

    //    def getNext(key: String, equal: Boolean, includeDelete: Boolean, options: Json): Option[Tuple2[String, String]] = {
    def getNext(key: String, equal: Boolean, includeDelete: Boolean, options: Json): Option[(String, String)] = {
      val key1 = info.storeTable.next(key, equal)
      key1 match {
        case Some(key2: String) => {
          info.storeTable.getMeta(key2) match {
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
                    info.storeTable.get(key2) match {
                      case Some(s: String) => s
                      case None => "null"
                    }
                  } else {
                    "null"
                  }
                  val result = getItems(key2, meta, v, get)
                  Some((key2, result))
                }
              }
            }
            case None => {
              // should not occur 
              None
            }
          }
        }
        case None => None
      }
    }

    //def getPrev(key: String, equal: Boolean, includeDelete: Boolean, options: Json): Option[Tuple2[String, String]] = {
    def getPrev(key: String, equal: Boolean, includeDelete: Boolean, options: Json): Option[(String, String)] = {
      val key1 = info.storeTable.prev(key, equal)
      key1 match {
        case (Some(key2: String)) => {
          info.storeTable.getMeta(key2) match {
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
                    info.storeTable.get(key2) match {
                      case Some(s: String) => s
                      case None => "null"
                    }
                  } else {
                    "null"
                  }
                  val result = getItems(key2, meta, v, get)
                  Some((key2, result))
                }
              }
            }
            case None => {
              // should not occur 
              None
            }
          }
        }
        case None => None
      }
    }
  }
}