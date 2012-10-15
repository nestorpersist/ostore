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
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

@RunWith(classOf[JUnitRunner])
class ChangeTest extends FunSuite {

  test("test1") {
    val dbName = "testdb"
    val serverConfig = Json("""
    {
     "store":{"class":"com.persist.store.Jdbm3","path":"/tmp/change"},
     "host":"127.0.0.1", "port":8011
    }        
    """)
    val databaseConfig = Json("""
    { 
     "tables":[ ],
     "rings":[ {"name":"r1",
       "nodes":[ {"name":"n1", "host":"127.0.0.1", "port":8011} ] } ]
     }
     """)

    val tableConfig = Json("""
         { "tables": [ {"name":"tab1"} ] }
     """)

    val nodeConfig2 = Json("""
    { 
     "rings":[ {"name":"r1",
       "nodes":[ {"name":"n2", "host":"127.0.0.1", "port":8011} ] } ]
     }
     """)
    val nodeConfig3 = Json("""
    { 
     "rings":[ {"name":"r1",
       "nodes":[ {"name":"n3", "host":"127.0.0.1", "port":8011} ] } ]
     }
     """)
    val nodeConfig1 = Json("""
    { 
     "rings":[ {"name":"r1",
       "nodes":[ {"name":"n1", "host":"127.0.0.1", "port":8011} ] } ]
     }
     """)
    val ringConfig = Json("""
    {
     "rings":[ {"name":"r2",
       "nodes":[ {"name":"n20", "host":"127.0.0.1", "port":8011},
                 {"name":"n21", "host":"127.0.0.1", "port":8011}] } ]
    }
    """)
    val ringConfig1 = Json("""
    {
     "rings":[ {"name":"r1"} ]
    }
    """)

    println("Starting Server")
    val server = new Server(serverConfig, true)

    val client = new Client()
    client.createDatabase(dbName, databaseConfig)

    val database = client.database(dbName)
    database.addTables(tableConfig)

    val tab1 = database.table("tab1")

    for (tableName <- database.allTables) {
      assert(tableName == "tab1", "table not added")
    }
    tab1.put("key1", "val1")
    tab1.put("key2", "val2")
    tab1.put("key3", "val3")
    tab1.put("key4", "val4")
    tab1.put("key5", "val5")
    tab1.delete("key3")

    def check(where: String) {
      println(where + ":" + Pretty(database.report("tab1")))
      val expect = List[String]("key1", "key2", "key4", "key5")

      for ((k, expectK) <- tab1.all().zip(expect)) {
        assert(k == expectK, where + " failed)" + k + ":" + expectK + ")")
      }
    }

    check("initial")

    database.addNodes(nodeConfig2)
    Thread.sleep(1000)
    check("add n2")

    database.addNodes(nodeConfig3)
    Thread.sleep(1000)
    check("add n3")
    
    database.addRings(ringConfig)
    Thread.sleep(1000)
    check("add r2")

    database.deleteNodes(nodeConfig1)
    Thread.sleep(1000)
    check("delete n1")

    database.deleteNodes(nodeConfig3)
    Thread.sleep(1000)
    check("delete n3")
    
    database.deleteRings(ringConfig1)
    Thread.sleep(1000)
    check("delete r1")

    database.deleteTables(tableConfig)
    for (tableName <- database.allTables) {
      assert(false, "table not deleted")
    }
    client.stopDatabase(dbName)
    client.deleteDatabase(dbName)

    client.stop()

    println("Stopping Server")
    server.stop
    println("DONE")
  }

}