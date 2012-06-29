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

import com.persist._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import JsonOps._
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class JsonTest extends FunSuite {

  test("json") {

    val s = """
      {"a":3, "b":"foo", "c":17, "d":[1,2,3,4,5],"e":false}
      """
    val j = Json(s)
    println("j:" + j)
    println("s:" + Compact(j))
    println("p:" + Pretty(j))

    val A = jfield("a")
    val B = jfield("b")
    val C = jfield("c")
    val F1 = jfield(1)
    val F2 = jfield(2)

    val a: Json = JsonArray(2, 3, "a")
    val m: Json = JsonObject("a" -> 3, "b" -> 4)
    val n = JsonObject("a" -> m, "c" -> a)

    println(jgetInt(m, "a") + ":" + jgetLong(m, "a"))
    for (i <- jgetArray(a)) {
      println("x:" + i)
    }

    for ((n, v) <- jgetObject(m)) {
      println(n + "->" + v)
    }

    val a2 = jgetString(a, 2)
    println(a2)
    val mv = jget(m, "a")
    println(mv)
    val nx = jget(n, "c", 0)
    println(nx)
    n match {
      case A(x: JsonObject) & A(B(a: Int)) & C(F2(b: String)) => println(x + ":" + a + ":" + b)
      case List("a", a) => println(a)
      case x => println("fail:" + x)
    }

  }
  
}