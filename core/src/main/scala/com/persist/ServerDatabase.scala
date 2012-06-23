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

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import JsonOps._
import akka.pattern._
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Await
import scala.collection.immutable.TreeMap

private[persist] class RingInfo(val name: String, val ring: ActorRef)

private[persist] class ServerDatabase(config:DatabaseConfig, serverConfig: Json, create: Boolean) extends Actor {
  val serverName = jgetString(serverConfig, "host") + ":" + jgetInt(serverConfig, "port")
  val databaseName = config.name
  val send = context.actorOf(Props(new Send(context.system,config)), name = "@send")
  var rings = TreeMap[String, RingInfo]()
  implicit val timeout = Timeout(5 seconds)

  def newRing(ringName: String) {
    val ring = context.actorOf(Props(new ServerRing(databaseName, ringName, send, config, serverConfig, create)), name = ringName)
    val f = ring ? ("start1")
    Await.result(f,5 seconds)
    val info = new RingInfo(ringName, ring)
    rings += (ringName -> info)
  }

  // TODO do in parallel
  for ((ringName,ringConfig)<-config.rings) {
    var hasRing = false
    for ((nodeName,nodeConfig)<-ringConfig.nodes) {
      if (nodeConfig.server.name == serverName) {
        hasRing = true
      }
    }
    if (hasRing) newRing(ringName)
  }

  def receive = {
    case ("start1") => {
      sender !  Codes.Ok 
    }
    case ("start2") => {
      for ((ringName, ringInfo) <- rings) {
        val f = ringInfo.ring ? ("start2")
        val v = Await.result(f, 5 seconds)
      }
      sender !  Codes.Ok 
    }
    case ("stop1") => {
      for ((ringName, ringInfo) <- rings) {
        val f = ringInfo.ring ? ("stop1")
        val v = Await.result(f, 5 seconds)
      }
      sender ! Codes.Ok
    }
    case ("stop2") => {
      for ((ringName, ringInfo) <- rings) {
        val f = ringInfo.ring ? ("stop2")
        val v = Await.result(f, 5 seconds)
      }
      val f = send ? ("stop")
      Await.result(f, 5 seconds)
      //val stopped = gracefulStop(send, 5 seconds)(context.system)
      //Await.result(stopped, 5 seconds)
      sender ! Codes.Ok
    }
    case x => println("databaseFail:" + x)
  }

}