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
class SimpleTest extends FunSuite {

  test("test with JDBM3 store") {
    val dbName = "testdb"
    val serverConfig = Json("""
    {
     "path":"/tmp/simple",
     "host":"127.0.0.1", "port":8011
    }        
    """)
    val databaseConfig = Json("""
    { 
     "tables":[ {"name":"tab1"} ],
     "rings":[ {"name":"r1",
       "nodes":[ {"name":"n1", "host":"127.0.0.1", "port":8011} ] } ]
     }
     """)

    println("Starting Server")
    Server.start(serverConfig, true)

    val system = ActorSystem("ostoreclient", ConfigFactory.load.getConfig("client"))
    val client = new Client(system)
    client.createDatabase(dbName, databaseConfig)

    val database = client.database(dbName)
    val tab1 = database.syncTable("tab1")
    println("tab1")
    tab1.put("key1", "val1")
    tab1.put("key2", "val2")
    tab1.put("key3", "val3")
    tab1.put("key4", "val4")
    tab1.put("key5", "val5")
    tab1.delete("key3")

    val v1 = tab1.get("key1")
    println("key1:" + v1)
    val v3 = tab1.get("key3")
    println("key3:" + v3)

    var expect = List("key1", "key2", "key4", "key5")
    var expectr = expect.reverse

    for (k <- tab1.all()) {
      println("fwd:" + k)
      assert(k == expect.head, "forward failed")
      expect = expect.tail
    }
    println("")
        for (k <- tab1.all(JsonObject("reverse" -> true))) {
      println("bwd:" + k)
      assert(k == expectr.head, "backward failed")
      expectr = expectr.tail
    }

    val report1 = database.report("tab1")
    println("Report:" + Pretty(report1))
    val monitor1 = database.monitor("tab1")
    println("Monitor:" + Pretty(monitor1))

    client.stopDataBase(dbName)
    client.deleteDatabase(dbName)

    client.stop()
    system.shutdown()

    println("Stopping Server")
    Server.stop
    println("DONE")
  }

  test("test with in-memory store") {
    val dbName = "testdb"
    val serverConfig = Json("""
    {
     "host":"127.0.0.1", "port":8011
    }
    """)
    val databaseConfig = Json("""
    {
     "tables":[ {"name":"tab1"} ],
     "rings":[ {"name":"r1",
       "nodes":[ {"name":"n1", "host":"127.0.0.1", "port":8011} ] } ]
     }
     """)

    println("Starting Server")
    Server.start(serverConfig, true)

    val system = ActorSystem("ostoreclient", ConfigFactory.load.getConfig("client"))
    val client = new Client(system)
    client.createDatabase(dbName, databaseConfig)

    val database = client.database(dbName)
    val tab1 = database.syncTable("tab1")
    println("tab1")
    tab1.put("key1", "val1")
    tab1.put("key2", "val2")
    tab1.put("key3", "val3")
    tab1.put("key4", "val4")
    tab1.put("key5", "val5")
    tab1.delete("key3")

    val v1 = tab1.get("key1")
    println("key1:" + v1)
    val v3 = tab1.get("key3")
    println("key3:" + v3)

    var expect = List("key1", "key2", "key4", "key5")
    var expectr = expect.reverse

    for (k <- tab1.all()) {
      println("fwd:" + k)
      assert(k == expect.head, "forward failed")
      expect = expect.tail
    }
    println("")
        for (k <- tab1.all(JsonObject("reverse" -> true))) {
      println("bwd:" + k)
      assert(k == expectr.head, "backward failed")
      expectr = expectr.tail
    }

    val report1 = database.report("tab1")
    println("Report:" + Pretty(report1))
    val monitor1 = database.monitor("tab1")
    println("Monitor:" + Pretty(monitor1))

    client.stopDataBase(dbName)
    client.deleteDatabase(dbName)

    client.stop()
    system.shutdown()

    println("Stopping Server")
    Server.stop
    println("DONE")
  }

}