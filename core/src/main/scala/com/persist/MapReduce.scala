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

/**
 * Map and reduce operations are defined by extending the traits
 * defined here.
 *
 * Classes that implement these traits can then be referenced
 * within table specifications of database configurations.
 *
 * Maps work by taking one or more source tables.
 * The map operation is applied to each item in the source
 * tables to produce zero or more items in the derived
 * map destination table.
 *
 * Reduces work by taking a single source table whose keys are Json arrays.
 * Items in the source table whose keys share a common prefix are
 * combined to produce the items in the derived reduce table.
 *
 * Derived map and reduce tables are read-only. Thats is, their items
 * may not be directly modified.
 *
 * Map and reduce are computed continuously and incrementally. When
 * a source item changes all values that depend upon it in derived tables
 * are changed. Derived tables become eventually consistent in near real-time.
 * 
 * Source-destination links can form cycles. This allows computation of
 * transitive closures and can be used to implement graph traversals.
 * 
 * If a database has multiple rings, derived map and reduce tables are
 * computed separately for each ring.
 *
 */
object MapReduce {

  /**
   * This is the parent trait of Map and Map2 traits.
   *
   * Every map must have an inverse that takes a destination key back
   * to the source key from which it was derived.
   * A simple way to guarantee an inverse is for the destination key to
   * to be an array that includes the source table key. When the source
   * key is an array, a common destination key is to reorder the elements of that
   * array.
   */
  trait MapAll {
    /**
     * Options passed in from the config file for the
     * map operation.
     */
    val options: Json
    /**
     * The inverse operation for a map.
     *
     * @param key a key that is in the map destination table.
     *
     * @return the key in the source table that was mapped to produce the destination key.
     */
    def from(key: JsonKey): JsonKey
  }

  /**
   * Extend this trait for maps.
   *
   * Classes that implement this
   * trait can be referenced in the act property of map specifications
   * within table configurations.
   */
  trait Map extends MapAll {
    /**
     * The mapping function.
     *
     * @param key the key of the source item.
     * @param value the value of the source item.
     *
     * @return a list of zero or more destination key-value pairs.
     */
    def to(key: JsonKey, value: Json): Traversable[(JsonKey, Json)]
  }

  /**
   * Extend this trait for prefix maps. Prefix maps are used to 
   * preform "joins".
   * 
   * Each derived map table can have one or more named prefix subtables.
   * An item in a prefix tables is associated with all those items in the main table
   * where the prefix table key is a prefix of the key of the main table item.
   *  
   * There will be a separate map derivation rules for the main table and
   * each of the subtables. Subtable maps are specified by including
   * the toprefix property in the table configuration. The value
   * of the toprefix property will be the name of the destination prefix subtable.
   * 
   * A prefix map will take as its source a map table and one of its prefix 
   * subtables. 
   * 
   * Classes that implement this
   * trait can be referenced in the act2 property of map specifications
   * within table configurations.
   */
  trait Map2 extends MapAll {
    /**
     * The prefix subtable name passed in from the config file.
     */
    val fromprefix: String
    /**
     * The prefix mapping function.
     * 
     * @param prefixKey the key of the source prefix subtable.
     * @param prefixValue the value of the source prefix subtable.
     * @param key the key of the source item.
     * @param value the value of the source item.
     *
     * @return a list of zero or more destination key-value pairs.
     */
    def to(prefixKey: JsonKey, prefixValue: Json, key: JsonKey, value: Json): Traversable[(JsonKey, Json)]
  }

  /**
   * Extend this trait for reduces.
   * Classes that implement this
   * trait can be referenced in the act property of reduce specifications
   * within table configurations.
   *
   * Items in the destination reduce table each have a ReduceKey and
   * a ReduceValue.
   *
   * ReduceKeys will be a Json array. All reduce key
   * arrays in a reduce table will have the same number of elements
   * (specified by size).
   * All items in the source table whose keys share the same
   * ReduceKey prefix are combined to produce the ReduceValue.
   *
   * Like all values ReduceValues are Json but all ReduceValues will
   * have a single logical algebraic ReduceType.  The reduce type
   * will have operations zero, add, and subtract that together define
   * an abelian group.
   */
  trait Reduce {
    /**
     * Options passed in from the config file for the
     * reduce operation.
     */
    val options: Json
    /**
     * The number of elements in the ReduceKey array.
     */
    val size: Int
    /**
     * The value of a reduce with no inputs. This is the identity element for the
     * abelian group.
     */
    val zero: Json
    /**
     * Takes the key and value of an item in the source table and
     * produces a value of the ReduceType.
     */
    def item(key: JsonKey, value: Json): Json
    /**
     * Takes two value with the ReduceType and combines them into
     * a new value of the ReduceType.
     * This is the operation for the abelian group.
     * This operation must be commutative and associative.
     */
    def add(value1: Json, value2: Json): Json
    /**
     * Takes two value with the ReduceType and combines them into
     * a new value of the ReduceType.
     * This operation is the inverse of the add operation.
     */
    def subtract(value1: Json, value2: Json): Json
  }

  private class MapIndex(val options: Json) extends Map {
    private val field = jgetString(options, "field")
    def to(key: JsonKey, value: Json) = {
      val newValue = jget(value, field)
      List((JsonArray(newValue, key), null))
    }
    def from(key: JsonKey): JsonKey = {
      jgetArray(key).tail.head
    }
  }

  private class MapIdentity(val options: Json) extends Map {
    def to(key: JsonKey, value: Json) = List((key, value))
    def from(key: JsonKey): JsonKey = key
  }

  private class MapOne(val options: Json) extends Map {
    def to(key: JsonKey, value: Json) = List((key, 1))
    def from(key: JsonKey): JsonKey = key
  }

  private class MapBoth(val options: Json) extends Map {
    def to(key: JsonKey, value: Json) = List((key, JsonArray(key, value)))
    def from(key: JsonKey): JsonKey = key
  }

  private class MapReverse(val options: Json) extends Map {
    def to(key: JsonKey, value: Json) = {
      List(key match {
        case a: JsonArray => (a.reverse, value)
        case _ => (key, value)
      })
    }
    def from(key: JsonKey): JsonKey = {
      key match {
        case a: JsonArray => a.reverse
        case _ => key
      }
    }
  }

  private class MapInvert(val options: Json) extends Map {
    def to(key: JsonKey, value: Json) = List((JsonArray(value, key), null))
    def from(key: JsonKey): JsonKey = jget(key, 1)
  }

  private class MapInvertKey(val options: Json) extends Map {
    def to(key: JsonKey, value: Json) = List((JsonArray(jget(key, 1), jget(key, 0)), value))
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
    def to(key: JsonKey, value: Json) = {
      val (cycle, first) = checkCycle(jgetArray(key))
      if (cycle && first) {
        List((key, value))
      } else {
        List()
      }
    }
    def from(key: JsonKey): JsonKey = key
  }

  private class MapRoute(val options: Json) extends Map {
    def to(key: JsonKey, value: Json) = {
      val r = jgetArray(key)
      val r1 = r.head
      val r2 = r.last
      var rx = r.tail.dropRight(1)
      List((JsonArray(r1, r2, rx), value))
    }
    def from(key: JsonKey): JsonKey = {
      val a = jgetArray(key)
      val r1 = jget(a, 0)
      val r2 = jget(a, 1)
      val rx = jgetArray(a, 2)
      r1 +: (rx :+ r2)
    }
  }

  private[persist] def getMap(options: Json): MapAll = {
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

  private class Map2Traverse(val options: Json, val fromprefix: String) extends Map2 {
    def to(prefixKey: JsonKey, prefixValue: Json, key: JsonKey, value: Json): Traversable[(JsonKey, Json)] = {
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

  private[persist] def getReduce(options: Json): Reduce = {
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
