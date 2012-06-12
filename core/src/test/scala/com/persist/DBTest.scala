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

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import JsonOps._
import org.scalatest.FunSuite
import net.kotek.jdbm.DBMaker
import java.util.SortedMap
import java.util.Map
import net.kotek.jdbm.DB
import java.io.File

@RunWith(classOf[JUnitRunner])
class DBTest extends FunSuite {

  test("db") {
    val dir = "test"
    val fname = dir + "/test"

    val f = new File(dir)
    if (f.exists()) {
      for (f1 <- f.listFiles()) {
        f1.delete()
      }
      f.delete()
    }
    // create directory
    f.mkdirs()
    
    val jv = Json("""{"a":3,  "b":false,"c":[2,3,4,5,6,7,8]}""")

    val db = DBMaker.openFile(fname).make()
    val v = db.createHashMap[String, String]("test")
    val j = db.createHashMap[String, JObject]("j")
    v.put("key", "val")
    v.put("key1","val1")
    j.put("j",new JObject(jv))
    db.commit()
    db.close()

    val db1 = DBMaker.openFile(fname).make()
    val v1 = db1.getHashMap[String, String]("test")
    val j1 = db1.getHashMap[String, JObject]("j")
    println(v1.get("key"))
    println(v1.get("key1"))
    val jv1 = j1.get("j").j
    println(Pretty(jv1))
    db1.commit()
    db1.close()
  }

}