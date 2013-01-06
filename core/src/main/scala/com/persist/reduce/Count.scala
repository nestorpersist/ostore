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

private[persist] class Average() extends com.persist.MapReduce.Reduce {
  val zero = JsonObject("cnt" -> 0L, "sum" -> 0.0)
  def in(key: JsonKey, value: Json): Json = JsonObject("cnt" -> 1, "sum" -> jgetDouble(value))
  def add(accum: Json, value: Json): Json =
    JsonObject("cnt" -> (jgetLong(accum, "cnt") + jgetLong(value, "cnt")),
      "sum" -> (jgetDouble(accum, "sum") + jgetDouble(value, "sum")))
  def subtract(accum: Json, value: Json): Json =
    JsonObject("cnt" -> (jgetLong(accum, "cnt") - jgetLong(value, "cnt")),
      "sum" -> (jgetDouble(accum, "sum") - jgetDouble(value, "sum")))
  def out(key: JsonKey, value: Json): Json = {
    val cnt = jgetLong(value, "cnt")
    if (cnt == 0) 0.0 else jgetDouble(value, "sum") / cnt
  }
}

