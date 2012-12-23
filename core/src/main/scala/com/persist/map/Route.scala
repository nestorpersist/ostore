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

package com.persist.map

import com.persist.JsonOps._
import com.persist.JsonKeys._

private[persist] class Route() extends com.persist.MapReduce.Map {
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
