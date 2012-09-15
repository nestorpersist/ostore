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

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.persist.JsonOps._
import com.persist.MapReduce._
import com.persist.Exceptions._
import scala.reflect

@RunWith(classOf[JUnitRunner])
class ReflectTest extends FunSuite {
  
  private def getMap(className:String):MapAll = {
    try {
      val c = Class.forName(className)
      println("c="+c)
      val obj = c.newInstance()
      obj.asInstanceOf[MapAll]
    } catch {
      case x=> throw InternalException(x.toString())
    }
  }
  
  private def tryMap(className:String):MapAll = {
    try
    try {
      getMap(className)
    } catch {
      case ex => println("fail:"+ex.toString()); null
    }
  }
  
  test("reflectTest") {
    val m = tryMap("com.persist.map.Index")
    m.options = JsonObject("a"->"b")
    println("options="+Compact(m.options))
    tryMap("com.persist.map.foo")
    tryMap("com.persist.unit.ReflectTest")
  }

}