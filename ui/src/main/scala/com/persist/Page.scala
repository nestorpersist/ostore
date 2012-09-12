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

class Page(client: WebClient) {

  // flat tree

  def up(database: String, table: String, key: Option[JsonKey], count: Int, parent: Option[JsonKey]): (Boolean, Seq[Json], Boolean) = {
    val keys = client.getKeyBatch(database, table, count + 1, key, true, false, parent)
    if (keys.size == 0) {
      val keys1 = client.getKeyBatch(database, table, count + 1, key, true, true, parent)
      if (keys1.size == count + 1) {
        (false, keys1.tail, true)
      } else {
        (false, keys1, false)
      }
    } else if (keys.size == count + 1) {
      val key1 = jget(keys, keys.size - 1)
      val keys1 = client.getKeyBatch(database, table, 1, Some(key1), false, true, parent)
      (true, keys.tail, keys1.size == 1)
    } else {
      val count1 = count + 1 - keys.size
      val key1 = jget(keys, keys.size - 1)
      val keys1 = client.getKeyBatch(database, table, count1, Some(key1), false, true, parent)
      if (keys1.size == count1) {
        (false, keys ++ keys1.dropRight(1), true)
      } else {
        (false, keys ++ keys1, false)
      }
    }
  }

  def down(database: String, table: String, key: Option[Json], count: Int, parent: Option[JsonKey]): (Boolean, Seq[Json], Boolean) = {
    val keys = client.getKeyBatch(database, table, count + 1, key, true, true, parent)
    if (keys.size == 0) {
      val keys1 = client.getKeyBatch(database, table, count + 1, key, true, false, parent)
      if (keys1.size == count + 1) {
        (true, keys1.tail, false)
      } else {
        (false, keys1, false)
      }
    } else if (keys.size == count + 1) {
      val key1 = jget(keys, 0)
      val keys1 = client.getKeyBatch(database, table, 1, Some(key1), false, false, parent)
      println("keys1="+Compact(keys1)+":"+Compact(keys))
      (keys1.size == 1, keys.dropRight(1), true)
    } else {
      val count1 = count + 1 - keys.size
      val key1 = jget(keys, 0)
      val keys1 = client.getKeyBatch(database, table, count1, Some(key1), false, false, parent)
      if (keys1.size == count1) {
        (true, keys1.tail ++ keys, false)
      } else {
        (false, keys1 ++ keys, false)
      }
    }
  }

}