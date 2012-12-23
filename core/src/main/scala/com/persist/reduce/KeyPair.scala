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

package com.persist.reduce

import com.persist.JsonOps._
import com.persist.JsonKeys._
import scala.collection.immutable.HashSet

private[persist] class KeyPair() extends com.persist.MapReduce.Reduce {
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
