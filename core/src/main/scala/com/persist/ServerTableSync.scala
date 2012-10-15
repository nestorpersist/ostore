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
import Codes.emptyResponse

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
          info.send ! ("sync", name, info.tableName, key, vals)
        }
      }
    }

    def toRing(ringName:String, key:String, meta:String, value:String) {
      val vals = (info.absentMetaS, Stores.NOVAL, meta, value)
      info.send ! ("sync", ringName, info.tableName, key, vals)
    }
    
    private def reconcile(cv1:Json, value1: Json, deleted1:Boolean,
                          cv2:Json, value2: Json, deleted2:Boolean,
                          cv0:Json, value0: Json, deleted0:Boolean):(Boolean, Json) = {
      val cmp1 = ClockVector.compare(cv1,cv0)
      val cmp2 = ClockVector.compare(cv2,cv0)
      val r = new Resolver()
      if ((cmp1 == '=' || cmp1 == '>') && (cmp2 == '=' || cmp2 == '>')) {
        // 3 way
        if (deleted1 && deleted2) {
          (true, null)
        } else if (deleted1 && ! deleted0) {
          (true, null)
        } else if (deleted2 && ! deleted0) {
          (true, null)
        } else if (deleted1 && deleted0) {
          (false, value2)
        } else if (deleted2 && deleted0) {
          (false, value1)
        } else {
          (false, r.resolve(value1, value2, Some(value0)))
        }
      } else {
        // 2 way
        if (deleted1 && deleted2) {
          (true, null)
        } else if (deleted1) {
          (false, JsonObject("$opt"->value2))
        } else if (deleted2) {
          (false, JsonObject("$opt"->value1))
        } else {
          (false, r.resolve(value1, value2, None))
        }
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
        case '=' => // already the same, nothing to do
        case '<' => // less than current, nothing to do
        case '>' => {
          // record the new value
          val value2 = info.storeTable.get(key) match {
            case Some(v) => v
            case None => Stores.NOVAL
          }
          info.storeTable.put(key, meta, v)
          map.doMap(key, oldMetaS, value2, Compact(cv1), v)
          reduce.doReduce(key, oldMetaS, value2, Compact(cv1), v)
        }
        case 'I' => {
          // conflict detected
          val cv0 = jget(Json(oldmeta),"c")
          val deleted1 = jgetBoolean(Json(meta),"d")
          val deleted2 = jgetBoolean(Json(oldMetaS),"d")
          val deleted0 = jgetBoolean(Json(oldmeta), "d")
          val Some(value2) = info.storeTable.get(key)
          val (deleted, value) = reconcile(cv1, Json(v), deleted1, cv2, Json(value2),deleted2, cv0, Json(oldv), deleted0)
          val cv = ClockVector.merge(cv1, cv2)
          val newValue = Compact(value)
          val dobj = if (deleted) JsonObject("d"->true) else JsonObject()
          val newMeta = Compact(JsonObject("c"->cv) ++ dobj)
          info.storeTable.put(key, newMeta, newValue)
          toRings(key, oldMetaS, value2, newMeta, newValue)
          map.doMap(key, oldMetaS, value2, newMeta, newValue)
          reduce.doReduce(key, oldMetaS, value2, newMeta, newValue)
        }
      }
      (Codes.Ok, emptyResponse)
    }

  }
}