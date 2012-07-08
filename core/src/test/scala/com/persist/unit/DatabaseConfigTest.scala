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
class DatabaseConfigTest extends FunSuite {

  test("json") {
  val cs = """
{ "tables":[
    {"name":"count2", "reduce":{"from":"tab2","act":"Count","size":1}},
    {"name":"icount2", "map":{"from":"count2","act":"Invert"}},
    {"name":"map3", "map":[
       {"from":"tab3","act":"Index","field":"a"},
       {"from":"tab2","act":"Index","field":"b"}]},
    {"name":"tab2"},
    {"name":"tab3"}
    ],
  "rings":[
   {"name":"r1",
    "nodes":[
    {"name":"n1","host":"h1","port":8011},
    {"name":"n2","host":"h2","port":8011}
    ]},
   {"name":"r2",
    "nodes":[
    {"name":"n3","host":"h3","port":8011}
    ]}
]}
    """
    val cj = Json(cs)
    val pcj = Pretty(cj)
    println(pcj)
    val config = DatabaseConfig("test", cj)
    val cj1 = config.toJson
    val pcj1 = Pretty(cj1)
    println(pcj1)
    assert(pcj == pcj1)
  }
  
}