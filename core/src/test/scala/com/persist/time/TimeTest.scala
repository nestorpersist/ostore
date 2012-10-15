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

package com.persist.time

import com.persist._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import JsonOps._
import org.scalatest.FunSuite
import java.io.File

@RunWith(classOf[JUnitRunner])
class TimeTest extends FunSuite {

  test("time1") {
    val dbName = "timedb"
    val serverConfig = Json("""
    {
     "store":{"class":"com.persist.store.Jdbm3","path":"/tmp/time"},
     "host":"127.0.0.1", "port":8011
    }        
    """)
    val databaseConfig = Json("""
    { 
     "tables":[ {"name":"beers"} ],
     "rings":[ {"name":"r1",
       "nodes":[ {"name":"n1", "host":"127.0.0.1", "port":8011} ] } ]
     }
     """)

    println("Starting Server")
    val server = new Server(serverConfig, true)

    val client = new Client()
    val system = client.system
    client.createDatabase(dbName, databaseConfig)

    val database = client.database(dbName)
    val beers = database.asyncTable("beers")

    val t1 = System.currentTimeMillis()

    Bulk.upload(new File("config/beer/beers.data"), beers)

    val t2 = System.currentTimeMillis()
    println("Total=" + (t2 - t1))

    val monitor1 = database.monitor("beers")
    println("Monitor:" + Pretty(monitor1))

    client.stopDatabase(dbName)
    client.deleteDatabase(dbName)

    client.stop()

    println("Stopping Server")
    server.stop
    println("DONE")
  }

}