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
import akka.actor.ActorSystem
import java.io.File
import java.io.Reader
import java.io.BufferedReader
import java.io.FileReader
import akka.actor.Actor
import akka.dispatch.Future
import akka.dispatch.DefaultPromise
import akka.util.duration._
import akka.util.Timeout
import akka.dispatch.Promise
import akka.actor.Props
import akka.dispatch.Await
import akka.pattern._
import com.typesafe.config.ConfigFactory

@RunWith(classOf[JUnitRunner])
class TimeTest extends FunSuite {
  
  val fast = JsonObject("fast"->true)
  val threads = 4

  class Run(cnt: Int, tab1: AsyncTable, f: Promise[Any]) extends Actor {
    implicit val executor = context.dispatcher
    for (i <- 1 to cnt) self ! "next"
    var done = 0
    val r = new BufferedReader(new FileReader(new File("config/genre.data")))
    var closed = false
    def receive = {
      case "next" => {
        val line = if (!closed) { r.readLine() } else { null }
        if (line == null) {
          if (!closed) {
            r.close()
            closed = true
          }
          done += 1
          println("done:"+done)
          if (done == cnt) {
            f.success("done")
          }
        } else if (line != "") {
          val parts = line.split("\t")
          val key = Json(parts(0))
          val value = Json(parts(1))
          val f = tab1.put(key, value,fast)
          f map {
            _ => self ! "next"
          }
        }
      }
    }
  }

  test("time1") {
    val dbName = "timedb"
    val serverConfig = Json("""
    {
     "path":"data",
     "host":"127.0.0.1", "port":8011
    }        
    """)
    val databaseConfig = Json("""
    { 
     "tables":[ {"name":"genre"} ],
     "rings":[ {"name":"r1",
       "nodes":[ {"name":"n1", "host":"127.0.0.1", "port":8011} ] } ]
     }
     """)

    println("Starting Server")
    Server.start(serverConfig,true)

    val system = ActorSystem("ostoreclient", ConfigFactory.load.getConfig("client"))
    val client = new Client(system)
    client.createDatabase(dbName, databaseConfig)

    val database = client.database(dbName)
    val tab1 = database.table("genre")
    val tab2 = database.asyncTable("genre")
    implicit val executor = system.dispatcher
    implicit val timeout = Timeout(2 hours)

    val t1 = System.currentTimeMillis()

    var f1 = new DefaultPromise[Any]
    val run = system.actorOf(Props(new Run(threads, tab2, f1)), name = "run")
    Await.result(f1, 2 hours)
    val stopped = gracefulStop(run, 10 seconds)(system)
    Await.result(stopped, 10 seconds)

    val t2 = System.currentTimeMillis()
    println("Total=" + (t2 - t1))

    val monitor1 = database.monitor("genre")
    println("Monitor:" + Pretty(monitor1))

    client.stopDataBase(dbName)
    client.deleteDatabase(dbName)

    client.stop()

    println("Stopping Server")
    Server.stop
    println("DONE")
  }

}