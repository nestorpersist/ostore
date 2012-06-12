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

import akka.actor.Actor
import akka.dispatch.Future
import Actor._
import JsonOps._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.DefaultPromise
import akka.util.duration._
import akka.dispatch._
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout

class SyncTable(tableName:String,system: ActorSystem, map: NetworkMap, send: ActorRef) {
  // TODO pass in config rather than default to ring r1
  // TODO option to get config from remote server

  implicit val executor = system.dispatcher

  val asyncClient = new AsyncTable(tableName,system, send)

  class All(options: Json) {
    // TODO fix so only 1 wait for entire loop
    def foreach(body: Json => Unit) = {
      var f = asyncClient.all(options)
      var done = false
      while (!done) {
        var r = Await.result(f, 20 seconds)
        r match {
          case Some(item) => {
            body(item.value)
            f = item.next()
          }
          case None => done = true
        }
      }
    }
  }

  def put(key: JsonKey, value: Json, options: Json = emptyJsonObject): Unit = {
    val f1 = asyncClient.put(key, value, options)
    Await.result(f1, 5 seconds)
  }

  def update(key: JsonKey, value: Json, vectorClock: Json, options: Json = emptyJsonObject): Boolean = {
    val f1 = asyncClient.update(key, value, vectorClock, options)
    Await.result(f1, 5 seconds)
  }

  def create(key: JsonKey, value: Json, options: Json = emptyJsonObject): Boolean = {
    val f1 = asyncClient.create(key, value, options)
    Await.result(f1, 5 seconds)
  }

  def delete(key: JsonKey, options: Json = emptyJsonObject): Unit = {
    val f1 = asyncClient.delete(key, options)
    Await.result(f1, 5 seconds)
  }

  def get(key: JsonKey, options: Json = emptyJsonObject): Option[Json] = {
    val f = asyncClient.get(key, options)
    val j = Await.result(f, 5 seconds)
    j
  }

  def all(options: Json = emptyJsonObject): All = {
    new All(options)
  }

  def report(): Json = {
    var result = JsonObject()
    for (ringName <- map.allRings) {
      var ro = JsonObject()
      val dest = Map("ring" -> ringName)
      // TODO could do calls in parallel
      for (nodeInfo <- map.allNodes(ringName)) {
        val name = nodeInfo.name
        val dest1 = dest + ("node" -> name)
        var f1 = new DefaultPromise[Any]
        send ! ("report", dest1, f1, tableName, "", "")
        val (code: String, x: String) = Await.result(f1, 5 seconds)
        ro = ro + (name->Json(x))
      }
      result = result + (ringName -> ro)
    }
    result
  }

  def monitor(): Json = {
    var result = JsonObject()
    for (ringName <- map.allRings) {
      var ro = JsonObject()
      val dest = Map("ring" -> ringName)
      // TODO could do calls in parallel
      for (nodeInfo <- map.allNodes(ringName)) {
        val name = nodeInfo.name
        val dest1 = dest + ("node" -> name)
        implicit val timeout = Timeout(5 seconds)
        val f1 = nodeInfo.monitorRef ? ("get", tableName)
        val (code: String, x: String) = Await.result(f1, 5 seconds)
        ro = ro + (name->Json(x))
      }
      result = result + (ringName -> ro)
    }
    result
  }
}