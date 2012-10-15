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

package com.persist.unit

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.persist._
import resolve._
import JsonOps._

@RunWith(classOf[JUnitRunner])
class ResolverTest extends FunSuite {

  test("resolve") {
    val v1 = Json(""" {"a":"aaa","b":"bbb","x":5.2, "s":[1,2,3]} """)
    val v2 = Json(""" {"a":"xxx","b":"bbb", "c":10, "s":[10,2,3]} """)
    val v3 = Json(""" {"a":"aaa","b":"yyy", "d":false,"s":[1,2,30]} """)
    val s1 = Json(""" {"$or":["a"]} """)
    val s2 = Json(""" {"$or":["a","b"]} """)
    val s3 = Json(""" {"$or":["b","c"]} """)
    val cv0 = ClockVector.empty
    val cv1 = ClockVector.incr(cv0, "a", 1)
    val cv2 = ClockVector.incr(cv0, "b", 1)
    val r3 = new Merge3()
    val r2 = new Merge2()
    println("T0:" + Pretty(r2.resolve2(1,cv1, "abc", false, cv2, "def", false)._2))
    println("T1:" + Pretty(r3.resolve3(2,cv1, v2, false, cv2, v3, false, cv0, v1, false)._2))
    println("T2:" + Pretty(r3.resolve3(3, cv1, v3, false, cv2, v2, false, cv0, v1, false)._2))
    println("T3:" + Pretty(r2.resolve2(4,cv1, v3, false, cv2, v2, false)._2))

    println("OR1:" + Pretty(r2.resolve2(5, cv1, s2, false, cv2, s3, false)._2))
    println("OR2:" + Pretty(r3.resolve3(6, cv1, s2, false, cv2, s3, false, cv0, s1, false)._2))
    println("OR3:" + Pretty(r2.resolve2(7, cv1, s2, false, cv2, "XYZ", false)._2))
  }

}