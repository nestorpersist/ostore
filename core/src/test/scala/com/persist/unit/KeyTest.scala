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

package com.persist.unit

import com.persist._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import JsonOps._
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class KeyTest extends FunSuite {

  private val keys = JsonArray(
    23,
    "foo",
    "99",
    JsonArray(23, "foo", "99"),
    JsonArray(23, JsonArray(1, 2, 3), JsonArray("a", "b", "c")))

  test("keys") {
    for (key <- keys) {
      val c = Compact(key)
      println("Checking: " + c)
      try {
        val key1 = Json(c)
        assert(keyEq(key, key1), "basic:" + c + ":" + Compact(key1))
        val p = Pretty(key)
        val key2 = Json(p)
        assert(keyEq(key, key2), "pretty:" + c + ":" + Compact(key2))
        val key3 = keyDecode(keyEncode(key))
        assert(keyEq(key, key3), "encode:" + c + ":" + Compact(key3))
        val key4 = keyUriDecode(keyUriEncode(key))
        assert(keyEq(key, key4), "uri:" + c + ":" + Compact(key4))
      } catch {
        case ex: Exception => {
          println("Failure of " + c + ":" + ex.toString())
          ex.printStackTrace()
          assert(false,"Failure of " + c + ":")
        }
      }
    }

  }

  private val order = JsonArray(
    JsonArray("a", "b"),
    JsonArray("a", "ab"),
    JsonArray(-10,-2),
    JsonArray(-10, 0),
    JsonArray(0, 10),
    JsonArray(2, 10),
    JsonArray("10", "2"),
    JsonArray(23, "23"),
    JsonArray("23", JsonArray("23")),
    JsonArray(JsonArray("a", "b"), JsonArray("b", "a")))

  test("key order") {
    for (vals <- order) {
      val key1 = jget(vals,0)
      val key2 = jget(vals,1)
      val c1 = Compact(key1)
      val c2 = Compact(key2)
      println("Order: " + c1 + ":" + c2)
      try {
        assert(keyLs(key1, key2), "order:" + c1 + ":" + c2)
      } catch {
        case ex: Exception => {
          println("Order Failure of " + c1 + ":" + c2 + ":" + ex.toString())
          ex.printStackTrace()
          assert(false, "Order Failure of " + c1 + ":" + c2)
        }
      }
    }
  }
  
  private val prefix = JsonArray(
      JsonArray("a", "ab",true),
      JsonArray(JsonArray(),JsonArray("a"),true),
      JsonArray(JsonArray("a"),JsonArray("a","b"),true),
      JsonArray("a","a",false),
      JsonArray(JsonArray("a"),JsonArray("a"),false),
      JsonArray(JsonArray("a"),JsonArray("ab"),false),
      JsonArray(JsonArray("a",JsonArray("b")),JsonArray("a",JsonArray("b","c")),false)
      )

  test("key prefixes") {
    for ( vals <- prefix) {
      val key1 = jget(vals,0)
      val key2 = jget(vals,1)
      val b = jgetBoolean(vals,2)
      val c1 = Compact(key1)
      val c2 = Compact(key2)
      println("Prefix: " + c1 + ":" + c2 + ":" + b)
      try {
        assert(keyPrefix(key1, key2) == b, "prefix:" + c1 + ":" + c2 +":" + b)
      } catch {
        case ex: Exception => {
          println("Prefix Failure of " + c1 + ":" + c2 + ":" + ex.toString())
          ex.printStackTrace()
          assert(false,"Prefix Failure of " + c1 + ":" + c2)
        }
      }
    }
  }

}