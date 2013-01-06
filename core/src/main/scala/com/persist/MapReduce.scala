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
import scala.collection.immutable.HashSet
import Exceptions._
import ExceptionOps._

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
     * The system initializes this variable. User code should not
     * change it.
     */
    var options: Json = emptyJsonObject
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
     * The system initializes this variable. User code should not
     * change it.
     */
    var fromprefix: String = ""
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
     * map operation.
     * The system initializes this variable. User code should not
     * change it.
     */
    var options: Json = emptyJsonObject
    /**
     * The number of elements in the ReduceKey array passed in from the config file.
     * The system initializes this variable. User code should not
     * change it.
     */
    var size: Int = 1
    /**
     * The value of a reduce with no inputs. This is the identity element for the
     * abelian group.
     */
    val zero: Json
    /**
     * Takes the key and value of an item in the source table and
     * produces a value of the ReduceType.
     * @param key the input key
     * @param value the input value
     *
     * @return the corresponding reduce value
     */
    def in(key: JsonKey, value: Json): Json
    /**
     * Takes two value with the ReduceType and combines them into
     * a new value of the ReduceType.
     * This is the operation for the abelian group.
     * This operation must be commutative and associative.
     * @param value1 the first reduce type value.
     * @param value2 the second reduce type value.
     *
     * @return the new reduce type value.
     */
    def add(value1: Json, value2: Json): Json
    /**
     * Takes two value with the ReduceType and combines them into
     * a new value of the ReduceType.
     * This operation is the inverse of the add operation.
     * @param value1 the first reduce type value.
     * @param value2 the second reduce type value.
     *
     * @return the new reduce type value.
     */
    def subtract(value1: Json, value2: Json): Json
    /**
     * Produce the destination value from the final combined
     * reduce type value.
     * @param key the key of the destination item.
     * @param value the reduce type value.
     *
     * @return the value to be stored in the destination item.
     */
    def out(key: JsonKey, value: Json): Json
  }

  private[persist] def getMap(className: String, options: Json): Map = {
    try {
      val c = Class.forName(className)
      val obj = c.newInstance()
      val map = obj.asInstanceOf[Map]
      map.options = options
      map
    } catch {
      case x => {
        val ex = InternalException(x.toString())
        println(ex.toString())
        throw ex
      }
    }
  }

  private[persist] def getMap2(className: String, options: Json, fromPrefix: String): Map2 = {
    try {
      val c = Class.forName(className)
      val obj = c.newInstance()
      val map = obj.asInstanceOf[Map2]
      map.options = options
      map.fromprefix = fromPrefix
      map
    } catch {
      case x => {
        val ex = InternalException(x.toString())
        println(ex.toString())
        throw ex
      }
    }
  }

  private[persist] def getMap(options: Json): MapAll = {
    val act = jgetString(options, "act")
    val act2 = jgetString(options, "act2")
    if (act != "") {
      getMap(act, options)
    } else if (act2 != "") {
      val fromprefix = jgetString(options, "fromprefix")
      getMap2(act2, options, fromprefix)
    } else {
      throw new Exception("no act specified")
    }
  }

  private def getReduce(className: String, options: Json, size: Int): Reduce = {
    try {
      val c = Class.forName(className)
      val obj = c.newInstance()
      val reduce = obj.asInstanceOf[Reduce]
      reduce.options = options
      reduce.size = size
      reduce
    } catch {
      case x => {
        val ex = InternalException(x.toString())
        println(ex.toString())
        throw ex
      }
    }
  }

  private[persist] def getReduce(options: Json): Reduce = {
    val act = jgetString(options, "act")
    val size = jgetInt(options, "size")
    getReduce(act, options, size)
  }
}
