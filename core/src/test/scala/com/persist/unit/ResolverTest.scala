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
import com.persist.Resolver
import com.persist.JsonOps._

@RunWith(classOf[JUnitRunner])
class ResolverTest extends FunSuite {
  
  test("resolve") {
    val v1 = Json(""" {"a":"aaa","b":"bbb","x":5.2, "s":[1,2,3]} """)
    val v2 = Json(""" {"a":"xxx","b":"bbb", "c":10, "s":[10,2,3]} """)
    val v3 = Json(""" {"a":"aaa","b":"yyy", "d":false,"s":[1,2,30]} """)
    val s1 = Json(""" {"$or":["a"]} """)
    val s2 = Json(""" {"$or":["a","b"]} """)
    val s3 = Json(""" {"$or":["b","c"]} """)
    val r = new Resolver()
    println("T0:"+Pretty(r.resolve("abc","def",None)))
    println("T1:"+Pretty(r.resolve(v2,v3,Some(v1))))
    println("T2:"+Pretty(r.resolve(v3,v2,Some(v1))))
    println("T3:"+Pretty(r.resolve(v3,v2,None)))
    
    println("OR1:"+Pretty(r.resolve(s2,s3,None)))
    println("OR2:"+Pretty(r.resolve(s2,s3,Some(s1))))
    println("OR3:"+Pretty(r.resolve(s2,"XYZ", None)))
  }

}