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

private[persist] trait ServerTableSyncComponent { this: ServerTableAssembly =>
  val sync: ServerTableSync
  class ServerTableSync {
    val hasSync = info.config.rings.size > 1
    var cntSync: Long = 0

    def toRings(key: String, oldMeta: String, oldValue: String, meta: String, value: String) {
      // send to other rings
      val vals = (oldMeta, oldValue, meta, value)
      for ((name,config) <- info.config.rings) {
        if (name != info.ringName) {
          val dest = Map("ring" -> name)
          val ret = "" // TODO will eventually hook this up
          info.send ! ("sync", dest, ret, info.tableName, key, vals)
        }
      }
    }

    def toRing(ringName:String, key:String, meta:String, value:String) {
      val dest = Map("ring" -> ringName)
      val vals = (info.absentMetaS, "null", meta, value)
      val ret = "" // TODO will eventually hook this up
      info.send ! ("sync", dest, ret, info.tableName, key, vals)
    }
    
    def reconcile(value1: Json, value2: Json): Json = {
      if (Compact(value1) == Compact(value2)) {
        value1
      } else {
        // TODO build alt tree
        // TODO incr vector clock
        value1
      }
    }

    def sync(key: String, oldmeta: String, oldv: String, meta: String, v: String): (String, Any) = {
      cntSync += 1
      val cv1 = jget(Json(meta), "c")
      val oldMetaS = info.storeTable.getMeta(key) match {
        case Some(s: String) => s
        case None => info.absentMetaS
      }
      val cv2 = jget(Json(oldMetaS), "c")
      ClockVector.compare(cv1, cv2) match {
        case '=' => {
          // already the same, nothing to do
        }
        case '<' => {
          // less than current, nothing to do
        }
        case '>' => {
          // record the new value
          val value2 = info.storeTable.get(key) match {
            case Some(v) => v
            case None => "null"
          }
          info.storeTable.putBoth(key, meta, v)
          mr.doMR(key, oldMetaS, value2, Compact(cv1), v)
        }
        case 'I' => {
          // conflict detected
          // check if values are equal
          val cv = ClockVector.merge(cv1, cv2)
          val Some(value2) = info.storeTable.get(key)
          val value = reconcile(Json(v), Json(value2))
          info.storeTable.putBoth(key, Compact(cv), Compact(value))
          toRings(key, oldMetaS, value2, Compact(cv), Compact(value))
          mr.doMR(key, oldMetaS, value2, Compact(cv), Compact(value))
        }
      }
      (Codes.Ok, "null")
    }

  }
}