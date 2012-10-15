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
import Stores._

private[persist] trait ServerTableReduceComponent { this: ServerTableAssembly =>
  val reduce: ServerTableReduce
  class ServerTableReduce {

    case class ReduceInfo(val to: String, val reduce: Reduce, val storeTable: StoreTable)

    var reduces: List[ReduceInfo] = Nil

    val tconfig = info.config.tables(info.tableName)
    val hasReduce = tconfig.fromReduce.size > 0

    for ((to, reduce) <- tconfig.fromReduce) {
      val rtname = info.tableName + "@" + to
      val reduceStore = info.store.getTable(rtname)
      val ri = ReduceInfo(to, getReduce(reduce), reduceStore)
      reduces = ri :: reduces
    }
    var fromReduce: Reduce = null
    for ((from, reduce) <- tconfig.toReduce) {
      ops.canWrite = false
      fromReduce = getReduce(reduce)
    }

    def close {
      for (ri <- reduces) {
        ri.storeTable.close()
      }
    }

    def delete {
      for (ri <- reduces) {
        ri.storeTable.store.deleteTable(ri.storeTable.tableName)
      }
    }

    def reduceOut(key: String, oldMeta: String, oldValue: String, meta: String, value: String) {
      // TODO handle balance
      val jkey = keyDecode(key)
      val jkeya = jkey match {
        case j: JsonArray => j
        case _ => return // key is not an array
      }
      // jkey must be an array
      val hasOld = !jgetBoolean(Json(oldMeta), "d")
      val joldValue = if (hasOld) Json(oldValue) else null
      val hasNew = !jgetBoolean(Json(meta), "d")
      val jvalue = if (hasNew) Json(value) else null
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
        val oldsum = ri.storeTable.get(keyEncode(prefix)) match {
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
            ri.storeTable.remove(keyEncode(prefix))
            ri.reduce.zero
          } else {
            ri.storeTable.put(keyEncode(prefix), info.absentMetaS, Compact(newsum))
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
          case None => NOVAL
        }
        info.storeTable.put(key, cmeta, csum)
        map.doMap(key, oldMetaS, oldSum, cmeta, csum)
        doReduce(key, oldMetaS, oldSum, cmeta, csum)
      }
      (Codes.Ok, emptyResponse)
    }

    def doReduce(key: String, oldMeta: String, oldValue: String, meta: String, value: String) {
      if (hasReduce) reduceOut(key, oldMeta, oldValue, meta, value)
    }
  }
}