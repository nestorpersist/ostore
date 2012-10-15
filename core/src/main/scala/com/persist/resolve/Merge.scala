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

package com.persist.resolve

import com.persist._
import JsonOps._
import scala.util.Sorting

private[persist] class Merge3 extends Resolve.Resolve3 {

  def init(config: Json) {}

  private def eq(i1: Json, i2: Json) = Compact(i1) == Compact(i2)

  private def or(i1: Json, i2: Json): Json = {
    if (Compact(i1) < Compact(i2)) {
      JsonObject("$or" -> JsonArray(i1, i2))
    } else {
      JsonObject("$or" -> JsonArray(i2, i1))
    }
  }

  private def opt(i1: Json): Json = {
    JsonObject("$opt" -> JsonArray(i1))
  }

  private def resolveNamed(i1: Option[Json], i2: Option[Json], old: Option[Json], hasOld: Boolean): Option[Json] = {
    (i1, i2, old) match {
      case (Some(v1), Some(v2), _) => Some(resolve(v1, v2, old))
      case (Some(v1), None, Some(oldv)) => {
        if (eq(v1, oldv)) {
          None
        } else {
          Some(opt(v1))
        }
      }
      case (None, Some(v2), Some(oldv)) => {
        if (eq(v2, oldv)) {
          None
        } else {
          Some(opt(v2))
        }
      }
      case (Some(v1), None, None) => {
        if (hasOld) {
          Some(v1)
        } else {
          Some(opt(v1))
        }
      }
      case (None, Some(v2), None) => {
        if (hasOld) {
          Some(v2)
        } else {
          Some(opt(v2))
        }
      }
      case (None, None, _) => None
    }
  }

  private def resolveObject(i1: JsonObject, i2: JsonObject, optionalOld: Option[JsonObject]): Json = {
    val keys = (i1.keys ++ i2.keys).toSet
    def getOld(key: String) = {
      optionalOld match {
        case Some(old) => old.get(key)
        case None => None
      }
    }
    val named = keys.map(key => (key, resolveNamed(i1.get(key), i2.get(key), getOld(key), optionalOld.isDefined)))
    val named1 = named.map(pair => {
      val (key, v) = pair
      v match {
        case Some(v1: Json) => JsonObject(key -> v1)
        case None => emptyJsonObject
      }
    })
    named1.fold(emptyJsonObject)(_ ++ _)
  }

  private def resolveArray(i1: JsonArray, i2: JsonArray, optionalOld: Option[JsonArray]): Json = {
    if (jsize(i1) == jsize(i2)) {
      optionalOld match {
        case Some(old) => {
          if (jsize(i1) == jsize(old)) {
            i1.zip(i2).zip(old).map(items => {
              val ((v1, v2), o) = items
              resolve(v1, v2, Some(o))
            })
          } else {
            i1.zip(i2).map(items => {
              val (v1, v2) = items
              resolve(v1, v2, None)
            })
          }
        }
        case None => {
          i1.zip(i2).map(items => {
            val (v1, v2) = items
            resolve(v1, v2, None)
          })
        }
      }

    } else {
      or(i1, i2)
    }
  }

  private def resolve1(i1: Json, i2: Json, optionalOld: Option[Json]): Json = {
    (i1, i2) match {
      case (v1: JsonObject, v2: JsonObject) => {
        optionalOld match {
          case Some(old: JsonObject) => resolveObject(v1, v2, Some(old))
          case _ => resolveObject(v1, v2, None)
        }
      }
      case (v1: JsonArray, v2: JsonArray) => {
        optionalOld match {
          case Some(old: JsonArray) => resolveArray(v1, v2, Some(old))
          case _ => resolveArray(v1, v2, None)
        }
      }
      case (v1, v2) => or(v1, v2)
    }
  }

  private def getSet(i: Json): (Boolean, Set[String]) = {
    val ior = jgetArray(i, "$or")
    val iopt = jgetArray(i, "$opt")
    if (jsize(ior) != 0) {
      (false, ior.map(Compact(_)).toSet)
    } else if (jsize(iopt) != 0) {
      (true, iopt.map(Compact(_)).toSet)
    } else {
      (false, Set(Compact(i)))
    }
  }

  private def resolve(i1: Json, i2: Json, optionalOld: Option[Json]): Json = {
    // Assumes the cv for old is less or equal to each of i1 and i2 cvs
    if (eq(i1, i2)) {
      i1
    } else {
      if (jget(i1, "$or") != null || jget(i1, "$opt") != null || jget(i2, "$or") != null || jget(i2, "opt") != null) {
        val (opt1, s1) = getSet(i1)
        val (opt2, s2) = getSet(i2)
        val (optold, sold) = optionalOld match {
          case Some(old) => getSet(old)
          case None => (false, Set[String]())
        }
        val s = (s1 ++ s2) -- (sold -- s1) -- (sold -- s2)
        val seq = s.toSeq
        val sort = Sorting.stableSort(seq).toSeq.map(Json(_))
        if (opt1 || opt2) {
          JsonObject("$opt" -> sort)
        } else {
          JsonObject("$or" -> sort)
        }
      } else {
        optionalOld match {
          case Some(old) => {
            if (eq(i1, old)) {
              i2
            } else if (eq(i2, old)) {
              i1
            } else {
              resolve1(i1, i2, optionalOld)
            }
          }
          case None => {
            resolve1(i1, i2, optionalOld)
          }
        }
      }
    }
  }

  def resolve3(key:Json, cv1: Json, value1: Json, deleted1: Boolean,
    cv2: Json, value2: Json, deleted2: Boolean,
    cv0: Json, value0: Json, deleted0: Boolean): (Boolean, Json) = {
    val cmp1 = ClockVector.compare(cv1, cv0)
    val cmp2 = ClockVector.compare(cv2, cv0)
    if ((cmp1 == '=' || cmp1 == '>') && (cmp2 == '=' || cmp2 == '>')) {
      // 3 way
      if (deleted1 && deleted2) {
        (true, null)
      } else if (deleted1 && !deleted0) {
        (true, null)
      } else if (deleted2 && !deleted0) {
        (true, null)
      } else if (deleted1 && deleted0) {
        (false, value2)
      } else if (deleted2 && deleted0) {
        (false, value1)
      } else {
        (false, resolve(value1, value2, Some(value0)))
      }
    } else {
      // 2 way
      if (deleted1 && deleted2) {
        (true, null)
      } else if (deleted1) {
        (false, JsonObject("$opt" -> value2))
      } else if (deleted2) {
        (false, JsonObject("$opt" -> value1))
      } else {
        (false, resolve(value1, value2, None))
      }
    }
  }
}

private[persist] class Merge2 extends Resolve.Resolve2 {
  val merge3 = new Merge3()
  def init(config: Json) {
    merge3.init(config)
  }
  def resolve2(key:Json, cv1: Json, value1: Json, deleted1: Boolean,
    cv2: Json, value2: Json, deleted2: Boolean): (Boolean, Json) = {
    merge3.resolve3(key, cv1, value1, deleted1,
      cv2, value2, deleted2,
      ClockVector.empty, null, true)
  }
}
