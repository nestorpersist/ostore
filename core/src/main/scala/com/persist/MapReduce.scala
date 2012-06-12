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
import scala.collection.immutable.HashSet

object MapReduce {
  
  // TODO document constraints on map and reduce
  // TODO reduce generalized to not just prefix but any key func

  trait MapAll {
    val options: Json
    def from(key: JsonKey): JsonKey
  }

  trait MapMany extends MapAll {
    def toMany(key: JsonKey, value: Json): Traversable[(JsonKey, Json)]
  }

  trait Map extends MapMany {
    def toMany(key: JsonKey, value: Json): Traversable[(JsonKey, Json)] = {
      val (key1, value1) = to(key, value)
      if (key1 == null) {
        List[(JsonKey, Json)]()
      } else {
        List[(JsonKey, Json)]((key1, value1))
      }
    }
    def to(key: JsonKey, value: Json): (JsonKey, Json)
  }

  trait Map2Many extends MapAll {
    val fromprefix: String
    def toMany(prefixKey: JsonKey, prefixValue: Json, key: JsonKey, value: Json): Traversable[(JsonKey, Json)]
  }

  trait Map2 extends Map2Many {
    def toMany(prefixKey: JsonKey, prefixValue: Json, key: JsonKey, value: Json): Traversable[(JsonKey, Json)] = {
      val (key1, value1) = to(prefixKey, prefixValue, key, value)
      if (key1 == null) {
        List[(JsonKey, Json)]()
      } else {
        List[(JsonKey, Json)]((key1, value1))
      }
    }
    def to(prefixKey: JsonKey, prefixValue: Json, key: JsonKey, value: Json): (JsonKey, Json)
  }

  trait Reduce {
    val options: Json
    val size: Int
    val zero: Json
    def item(key: JsonKey, value: Json): Json
    def add(accum: Json, value: Json): Json
    def subtract(accum: Json, value: Json): Json
  }

  private class MapIndex(val options: Json) extends Map {
    private val field = jgetString(options, "field")
    def to(key: JsonKey, value: Json): (JsonKey, Json) = {
      val newValue = jget(value, field)
      (JsonArray(newValue, key), null)
    }
    def from(key: JsonKey): JsonKey = {
      jgetArray(key).tail.head
    }
  }

  private class MapIdentity(val options: Json) extends Map {
    def to(key: JsonKey, value: Json): (JsonKey, Json) = (key, value)
    def from(key: JsonKey): JsonKey = key
  }

  private class MapOne(val options: Json) extends Map {
    def to(key: JsonKey, value: Json): (JsonKey, Json) = (key, 1)
    def from(key: JsonKey): JsonKey = key
  }

  private class MapBoth(val options: Json) extends Map {
    def to(key: JsonKey, value: Json): (JsonKey, Json) = (key, JsonArray(key, value))
    def from(key: JsonKey): JsonKey = key
  }

  private class MapReverse(val options: Json) extends Map {
    def to(key: JsonKey, value: Json): (JsonKey, Json) = {
      key match {
        case a: JsonArray => (a.reverse, value)
        case _ => (key, value)
      }
    }
    def from(key: JsonKey): JsonKey = {
      key match {
        case a: JsonArray => a.reverse
        case _ => key
      }
    }
  }

  private class MapInvert(val options: Json) extends Map {
    def to(key: JsonKey, value: Json): (JsonKey, Json) = (JsonArray(value, key), null)
    def from(key: JsonKey): JsonKey = jget(key, 1)
  }

  private class MapInvertKey(val options: Json) extends Map {
    def to(key: JsonKey, value: Json): (JsonKey, Json) = (JsonArray(jget(key, 1), jget(key, 0)), value)
    def from(key: JsonKey): JsonKey = JsonArray(jget(key, 1), jget(key, 0))
  }

  private def checkCycle(a: JsonArray): (Boolean, Boolean) = {
    var first: String = ""
    var last: String = ""
    var min: String = ""
    var i: Int = 0
    for (elem <- a) {
      val s = keyEncode(elem)
      if (i == 0) {
        first = s
        min = s
      }
      last = s
      if (s < min) min = s
      i += 1
    }
    (first == last, first == min)
  }

  private class MapCycle(val options: Json) extends Map {
    def to(key: JsonKey, value: Json): (JsonKey, Json) = {
      val (cycle, first) = checkCycle(jgetArray(key))
      if (cycle && first) {
        (key, value)
      } else {
        (null, null)
      }
    }
    def from(key: JsonKey): JsonKey = key
  }

  private class MapRoute(val options: Json) extends Map {
    def to(key: JsonKey, value: Json): (JsonKey, Json) = {
      val r = jgetArray(key)
      val r1 = r.head
      val r2 = r.last
      var rx = r.tail.dropRight(1)
      (JsonArray(r1, r2, rx), value)
    }
    def from(key: JsonKey): JsonKey = {
      val a = jgetArray(key)
      val r1 = jget(a, 0)
      val r2 = jget(a, 1)
      val rx = jgetArray(a, 2)
      r1 +: (rx :+ r2)
    }
  }

  def getMap(options: Json): MapAll = {
    val act = jgetString(options, "act")
    val act2 = jgetString(options, "act2")
    if (act != "") {
      act match {
        case "Identity" => new MapIdentity(options)
        case "Index" => new MapIndex(options)
        case "One" => new MapOne(options)
        case "Both" => new MapBoth(options)
        case "Reverse" => new MapReverse(options)
        case "InvertKey" => new MapInvertKey(options)
        case "Invert" => new MapInvert(options)
        case "Cycle" => new MapCycle(options)
        case "Route" => new MapRoute(options)
        case "TextIndex" => new Text.MapTextIndex(options)
        case x => {
          // TODO use reflection to find class
          throw new Exception("unknown map act: " + x)
        }
      }
    } else if (act2 != "") {
      val fromprefix = jgetString(options, "fromprefix")
      act2 match {
        case "Traverse2" => new Map2Traverse(options, fromprefix)
        case x => {
          // TODO use reflection to find class
          throw new Exception("unknown map act2: " + x)
        }
      }
    } else {
      throw new Exception("no act specified")
    }
  }

  private class Map2Traverse(val options: Json, val fromprefix: String) extends Map2Many {
    def toMany(prefixKey: JsonKey, prefixValue: Json, key: JsonKey, value: Json): Traversable[(JsonKey, Json)] = {
      val keya = jgetArray(key)
      val prefixKeya = jgetArray(prefixKey)
      val (cycle, first) = checkCycle(keya)
      var result = List[(JsonKey, Json)]()
      if (!cycle) {
        for (elem <- jgetArray(prefixValue)) {
          if (!keya.dropRight(1).contains(elem)) {
            val newKey: Json = elem +: keya
            result = (newKey, value) +: result
          }
        }
      }
      result
    }
    def from(key: JsonKey): JsonKey = jgetArray(key).tail
  }

  private class ReduceCount(val options: Json, val size: Int) extends Reduce {
    val zero = 0
    def item(key: JsonKey, value: Json): Json = 1
    def add(accum: Json, value: Json): Json = jgetLong(accum) + jgetLong(value)
    def subtract(accum: Json, value: Json): Json = jgetLong(accum) - jgetLong(value)
  }

  private class ReduceKeyPair(val options: Json, val size: Int) extends Reduce {
    val zero = JsonArray()
    def item(key: JsonKey, value: Json): Json = jgetArray(key).takeRight(1)
    def add(accum: Json, value: Json): Json = {
      val set = new HashSet[Json]() ++ jgetArray(accum) ++ jgetArray(value)
      set.toList
    }
    def subtract(accum: Json, value: Json): Json = {
      val set = new HashSet[Json]() ++ jgetArray(accum) -- jgetArray(value)
      set.toList
    }
  }

  def getReduce(options: Json): Reduce = {
    val act = jgetString(options, "act")
    val size = jgetInt(options, "size")
    act match {
      case "Count" => new ReduceCount(options, size)
      case "KeyPair" => new ReduceKeyPair(options, size)
      case x => {
        // TODO use reflection to find class
        throw new Exception("unknown reduce: " + x)
      }
    }
  }
}
