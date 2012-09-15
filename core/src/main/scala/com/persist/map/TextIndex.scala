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
import com.persist.Text.Words

private[persist] class TextIndex() extends com.persist.MapReduce.Map {
  def to(key: JsonKey, value: Json): Traversable[(JsonKey, Json)] = {
    val title = jgetString(key)
    val words = jgetString(value)
    val s1 = {
      val p1 = new Words("", title)
      p1.parseAll(p1.all, words).get
    }
    val s2 = {
      val p2 = new Words("title", title)
      p2.parseAll(p2.all, title).get
    }
    s1 ::: s2
  }
  def from(key: JsonKey): JsonKey = jgetArray(key, 1)
}
