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

private[persist] object ClockVector {

  // {"r1:"d1,"r2":d2} can be empty but not null

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

  val empty = emptyJsonObject

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

  def incr(cv: Json, rn: String, d: Long): Json = {
    var result = jgetObject(cv)
    val oldd = jgetLong(result, rn)
    val newd = List(oldd + 1, d).max
    result = result + (rn -> newd)
    result
  }

}