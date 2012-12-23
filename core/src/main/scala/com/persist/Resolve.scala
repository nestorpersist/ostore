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
import Exceptions._
import ExceptionOps._

/**
 * This object contains the traits needed to define 2-way and 3-way conflict
 * resolvers. Built-in implementations can be found in package com.persist.resolve.
 */
object Resolve {

  private[persist] object Resolve2 {
    def apply(config: Json): Resolve2 = {
      try {
        val className = jgetString(config, "class")
        val className1 = if (className == "") "com.persist.resolve.Merge" else className
        val c = Class.forName(className1 + "2")
        val obj = c.newInstance()
        val resolve2 = obj.asInstanceOf[Resolve2]
        resolve2.init(config)
        resolve2
      } catch {
        case x => {
          val ex = InternalException(x.toString())
          throw ex
        }
      }
    }
  }

  private[persist] object Resolve3 {
    def apply(config: Json): Resolve3 = {
      try {
        val className = jgetString(config, "class")
        val className1 = if (className == "") "com.persist.resolve.Merge" else className
        val c = Class.forName(className1 + "3")
        val obj = c.newInstance()
        val resolve3 = obj.asInstanceOf[Resolve3]
        resolve3.init(config)
        resolve3
      } catch {
        case x => {
          val ex = InternalException(x.toString())
          throw ex
        }
      }
    }
  }

  /**
   * This trait should be implemented to build a custom 2-way conflict resolver.
   */
  trait Resolve2 {
    /**
     * This method is called once before any other method calls.
     *
     * @param config the resolve2 configuration.
     */
    def init(config: Json)

    /**
     * Called to do a three way resolution. The order of the items does not
     * mater.
     *
     * @param key the key for both items.
     * @param cv1 the clock vector for the first item.
     * @param value1 the value of the first item.
     * @param deleted1 true if the first item has been deleted.
     * @param cv2 the clock vector for the second item.
     * @param value2 the value of the second item.
     * @param deleted2 true if the second item has been deleted.
     *
     * @return whether the value should be deleted and its new value.
     */
    def resolve2(key: Json, cv1: Json, value1: Json, deleted1: Boolean,
      cv2: Json, value2: Json, deleted2: Boolean): (Boolean, Json)
  }

  /**
   * This trait should be implemented to build a custom 3-way conflict resolver.
   */
  trait Resolve3 {
    /**
     * This method is called once before any other method calls.
     *
     * @param config the resolve2 configuration.
     */
    def init(config: Json)

    /**
     * Called to do a three way resolution. The order of the first and second item does not
     * mater. The third (0) item is the common ancestor of the first and second item.
     *
     * cv1 >= cv0 && cv2 >= cv0
     *
     * @param key the key for both items.
     * @param cv1 the clock vector for the first item.
     * @param value1 the value of the first item.
     * @param deleted1 true if the first item has been deleted.
     * @param cv2 the clock vector for the second item.
     * @param value2 the value of the second item.
     * @param deleted2 true if the second item has been deleted.
     * @param cv0 the clock vector for the ancestor item.
     * @param value0 the value of the ancestor item.
     * @param deleted0 true if the ancestor item has been deleted.
     *
     * @return whether the value should be deleted and its new value.
     */
    def resolve3(key: Json, cv1: Json, value1: Json, deleted1: Boolean,
      cv2: Json, value2: Json, deleted2: Boolean,
      cv0: Json, value0: Json, deleted0: Boolean): (Boolean, Json)
  }

}
