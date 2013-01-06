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

import akka.actor.Actor
import akka.actor.ActorRef
import JsonOps._
import JsonKeys._
import Codes.emptyResponse

private[persist] trait ServerTableSyncComponent { this: ServerTableAssembly =>
  val sync: ServerTableSync
  class ServerTableSync {
    val hasSync = info.config.rings.size > 1
    var cntSync: Long = 0

    val tconfig = info.config.tables(info.tableName)
    val config3 = tconfig.resolve3
    val config2 = tconfig.resolve2
    val (resolve2, resolve3, threeWay) = {
      if (config3 != null) {
        (null, Resolve.Resolve3(config3), true)
      } else if (config2 != null) {
        (Resolve.Resolve2(config2), null, false)
      } else {
        (null, Resolve.Resolve3(null), true)
      }
    }

    def toRings(key: String, oldMeta: String, oldValue: String, meta: String, value: String, id: Long, options: String) {
      // send to other rings
      val vals = (options, oldMeta, oldValue, meta, value)
      for ((name, config) <- info.config.rings) {
        if (name != info.ringName) {
          info.send ! ("sync", name, id, info.tableName, key, vals)
        }
      }
    }

    def toRing(ringName: String, key: String, meta: String, value: String, options: String) {
      val vals = (options, info.absentMetaS, Stores.NOVAL, meta, value)
      info.send ! ("sync", ringName, 0L, info.tableName, key, vals)
    }

    def sync(key: String, oldmeta: String, oldv: String, meta: String, v: String, os: String): (String, Any) = {
      val options = Json(os)
      val fast = jgetBoolean(options, "fast")
      cntSync += 1
      val cv1 = jget(Json(meta), "c")
      val oldMetaS = info.storeTable.getMeta(key) match {
        case Some(s: String) => s
        case None => info.absentMetaS
      }
      val cv2 = jget(Json(oldMetaS), "c")
      ClockVector.compare(cv1, cv2) match {
        case '=' => // already the same, nothing to do
        case '<' => // less than current, nothing to do
        case '>' => {
          // record the new value
          val value2 = info.storeTable.get(key) match {
            case Some(v) => v
            case None => Stores.NOVAL
          }
          info.storeTable.put(key, meta, v, fast)
          map.doMap(key, oldMetaS, value2, Compact(cv1), v)
          reduce.doReduce(key, oldMetaS, value2, Compact(cv1), v)
        }
        case 'I' => {
          // conflict detected
          val cv0 = jget(Json(oldmeta), "c")
          val deleted1 = jgetBoolean(Json(meta), "d")
          val deleted2 = jgetBoolean(Json(oldMetaS), "d")
          val Some(value2) = info.storeTable.get(key)
          val (deleted, value) = if (threeWay) {
            val deleted0 = jgetBoolean(Json(oldmeta), "d")
            resolve3.resolve3(keyDecode(key), cv1, Json(v), deleted1, cv2, Json(value2), deleted2, cv0, Json(oldv), deleted0)
          } else {
            resolve2.resolve2(keyDecode(key), cv1, Json(v), deleted1, cv2, Json(value2), deleted2)
          }
          val cv = ClockVector.merge(cv1, cv2)
          val newValue = if (deleted) Stores.NOVAL else Compact(value)
          val dobj = if (deleted) JsonObject("d" -> true) else JsonObject()
          val newMeta = Compact(JsonObject("c" -> cv) ++ dobj)
          info.storeTable.put(key, newMeta, newValue, fast)
          toRings(key, oldMetaS, value2, newMeta, newValue, 0, emptyResponse)
          map.doMap(key, oldMetaS, value2, newMeta, newValue)
          reduce.doReduce(key, oldMetaS, value2, newMeta, newValue)
        }
      }
      (Codes.Ok, emptyResponse)
    }

  }
}