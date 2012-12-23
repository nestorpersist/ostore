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

private[persist] object Cycle {
  private[persist] def checkCycle(a: JsonArray): (Boolean, Boolean) = {
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
}
private[persist] class Cycle() extends com.persist.MapReduce.Map {
  def to(key: JsonKey, value: Json) = {
    val (cycle, first) = Cycle.checkCycle(jgetArray(key))
    if (cycle && first) {
      List((key, value))
    } else {
      List()
    }
  }
  def from(key: JsonKey): JsonKey = key
}
