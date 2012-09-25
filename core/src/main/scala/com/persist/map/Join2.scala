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

private[persist] class Join2() extends com.persist.MapReduce.Map2 {
  def to(prefixKey: JsonKey, prefixValue: Json, key: JsonKey, value: Json): Traversable[(JsonKey, Json)] = {
    List((key, jgetObject(value) ++ jgetObject(prefixValue)))
  }
  def from(key: JsonKey): JsonKey = key
}
