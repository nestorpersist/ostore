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

/**
 * This object contains operations on ClockVectors, the OStore implementation of 
 * vector clocks.
 * OStore uses ClockVectors for conflict detection and resolution.
 * 
 * ClockVectors are represented by Json Objects. The object
 * contains a set of name-value pairs where the name is the name
 * of the ring where a change was made and the value is a long
 * integer which is the millisecond time when that change was made.
 */
object ClockVector {

  private def getRingNames(cv1: Json, cv2: Json) = {
    var result = Set[String]()
    val it1 = jgetObject(cv1).keys
    val it2 = jgetObject(cv2).keys
    for (n <- it1) {
      result += n
    }
    for (n <- it2) {
      result += n
    }
    result
  }

  /**
   * The empty clock vector.
   */
  val empty:Json = emptyJsonObject

  /**
   * Determines which clock vector was last modified.
   * 
   * @param cv1 the first clock vector.
   * @param cv2 the second clock vector.
   * @return true if cv1 was modified before cv2.
   */
  def newer(cv1: Json, cv2: Json): Boolean = {
    var max: Long = 0
    var result = true
    for (n <- getRingNames(cv1, cv2)) {
      val d1: Long = jgetLong(cv1, n)
      val d2: Long = jgetLong(cv2, n)
      if (d1 > d2 && d1 > max) {
        max = d1
        result = true
      } else if (d2 > max) {
        max = d2
        result = false
      }
    }
    false
  }

  /**
   * Compares two clock vectors.
   * 
   * @param cv1 the first clock vector.
   * @param cv2 the second clock vector.
   * @return the result of the comparison
   *  - '<' if cv1 < cv2
   *  - '>' if cv1 > cv2
   *  - '=' if cv1 == cv2
   *  - 'I' otherwise
   */
  def compare(cv1: Json, cv2: Json): Char = {
    var ls = false
    var gt = false

    for (n <- getRingNames(cv1, cv2)) {
      val d1: Long = jgetLong(cv1, n)
      val d2: Long = jgetLong(cv2, n)
      if (d1 < d2) ls = true
      if (d1 > d2) gt = true
    }
    (ls, gt) match {
      case (true, true) => 'I'
      case (true, _) => '<'
      case (_, true) => '>'
      case (false, false) => '='
    }
  }

  /**
   * Merge two clock vectors.
   *  
   * @param cv1 the first clock vector.
   * @param cv2 the second clock vector.
   * @return the smallest clock vector that is greater than both cv1 and cv2
   */
  def merge(cv1: Json, cv2: Json): Json = {
    var result = JsonObject()
    for (n <- getRingNames(cv1, cv2)) {
      val d1: Long = jgetLong(cv1, n)
      val d2: Long = jgetLong(cv2, n)
      val d = List(d1, d2).max
      result = result + (n -> d)
    }
    result
  }

  /** 
   * Increment a clock vector
   * 
   * @param cv a vector clock
   * @param ringName the name of a ring
   * @param the current millisecond time
   * @return a new clock vector whose ringName component value is max of d and the current value + 1.
   */
  def incr(cv: Json, ringName: String, t: Long): Json = {
    var result = jgetObject(cv)
    val oldd = jgetLong(result, ringName)
    val newd = List(oldd + 1, t).max
    result = result + (ringName -> newd)
    result
  }

}