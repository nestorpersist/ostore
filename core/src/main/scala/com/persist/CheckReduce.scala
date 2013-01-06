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

private[persist] class CheckReduce(client: Client, cmd: String, config: Json) {

  private[this] val now = System.currentTimeMillis()
  private[this] val fix = cmd == "fix"

  private def loc(table: String, key: JsonKey, value: Json) = table + ":(" + Compact(key) + "," + Compact(value) + ")"

  private class ReduceDest(table: Table, ringOption: JsonObject, ring: String, from: String) {
    private[this] var all = table.all(ringOption + ("get" -> "kv")).iterator
    private[this] var old: Json = null
    def check(k: Json, v: Json) {
      if (k == null) return
      if (all.hasNext) {
        val kv1 = all.next
        val k1 = jget(kv1, "k")
        val v1 = jget(kv1, "v")
        //println("***" + Compact(k) + ":" + Compact(v) + " => " + Compact(k1) + ":" + Compact(v1))
        val cmp = keyCompare(k, k1)
        if (cmp == 0) {
          if (Compact(v1) != Compact(v)) {
            // error bad value
            println("Wrong destination item value " + loc(table.tableName, k1, v1) + " from " + loc(from, k, v) + " on ring " +ring)
            // TODO fix
          }
        } else if (cmp < 0) {
          println("Missing destination item " + loc(table.tableName, k, v) + " on ring " + ring)
          // TODO fix
        } else {
          println("Extra destination item " + loc(table.tableName, k1, v1) + " on ring " + ring)
          // TODO fix
          check(k, v)
        }
      } else {
        println("Missing destination item " + loc(table.tableName, k, v) + " on ring " + ring)
        // TODO fix
      }
    }
    def done {
      if (all.hasNext) {
        val kv1 = all.next
        val k1 = jget(kv1, "k")
        val v1 = jget(kv1, "v")
        println("Extra destination item " + loc(table.tableName, k1, v1) + " on ring " + ring)
        done
      }
    }
  }

  private def checkReduces1(database: Database, table: Table, t: Json, ring: String, ringOption: JsonObject) {
    for (reduce <- jgetArray(t, "reduce")) {
      val from = jgetString(reduce, "from")
      val size = jgetInt(reduce, "size")
      val act = jgetString(reduce, "act")
      println("Reduce of " + from + "=>" + table.tableName + " on ring " + ring)
      val reducer = MapReduce.getReduce(reduce)
      val dest = new ReduceDest(table, ringOption, ring, from)
      var v1: Json = reducer.zero
      val srcTable = database.table(from)
      var currentPrefix: Json = null
      for (kv <- srcTable.all(ringOption + ("get" -> "kv"))) {
        val k = jgetArray(kv, "k")
        val v = jget(kv, "v")
        val prefix = if (k.size >= size) k.take(size) else null
        if (prefix != null) {
          if (currentPrefix != prefix) {
            dest.check(currentPrefix, v1)
            currentPrefix = prefix
            v1 = reducer.zero
          }
          // add to set
          v1 = reducer.add(v1, reducer.in(k, v))
          //println(Compact(k) + ":" + Compact(v))
        }
      }
      dest.check(currentPrefix, reducer.out(currentPrefix, v1))
      // check for any extras
      dest.done
    }
  }

  def checkReduces(database: Database, table: Table, t: Json) {
    for (ring <- table.database.allRings) {
      val ringOption = JsonObject("ring" -> ring)
      checkReduces1(database, table, t, ring, ringOption)
    }
  }
}