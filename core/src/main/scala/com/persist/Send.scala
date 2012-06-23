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

import scala.collection.immutable.TreeMap
import JsonOps._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch._
import akka.actor.Cancellable

private[persist] class Send(system:ActorSystem,config:DatabaseConfig) extends Actor {
  
  val databaseMap = DatabaseMap(config)
  
  val timeout1 = 100000L // debug 100 sec
  implicit val timeout = Timeout(5 seconds)

  private var last = 0L
  private val uidGen = new UidGen
  
  val defaultRing = config.rings.keys.head

  class MsgAction(val ring: String, val nodeMap: NodeMap, val table: String, val when: Long, val why: String)
  
  private val noDest = Map[String,String]()

  class MsgInfo(kind1: String, dest1: Map[String, String], client1: Any, uid1: Long, tab1: String, key1: String, value1: Any) {
    val kind = kind1
    val dest = dest1
    val client = client1
    val uid = uid1
    val tab = tab1
    val key = key1
    val value = value1
    var history = List[MsgAction]() // most recent first
  }

  var events = TreeMap[Long, Long]()

  var timer: Cancellable = null
  var timerWhen = 0L

  def setTimer {
    if (events.size > 0) {
      val when = events.keySet.firstKey
      val now: Long = System.currentTimeMillis
      if (timer != null) {
        if (when == timerWhen) return // use previously scheduled timer
        // cancel previous timer
        timer.cancel()
        timer = null
      }
      if (when > now) {
        val delta: Long = when - now
        timer = system.scheduler.scheduleOnce(5 milliseconds, null, "foo")
        //("timer"),when-now,java.util.concurrent.TimeUnit.MILLISECONDS)
        timerWhen = when
      } else {
        self ! ("timer")
      }
    }
  }

  def addEvent(when: Long, ts: Long) {
    events = events + (when -> ts)
    setTimer
  }

  var msgs = Map[Long, MsgInfo]()

  def sendClient(info: MsgInfo, kind: String, response: String) {
    info.client match {
      case c: ActorRef => {
        c ! (kind, response)
      }
      case f: Promise[Any] => {
        if (f != null) f.success((kind, response))
      }
      case "" => // ignore response
      case _ => println("Unrecognized client type")
    }
    msgs = msgs - info.uid
  }

  private def sendDirect(kind: String, ring: String, nodeMap:NodeMap, client: ActorRef, uid: Long, tab: String, key: String, value: Any) {
    //val dest = networkMap.get(ring, server, tab)
    val ref = nodeMap.getRef(system)
    ref ! (kind, uid, key, value)
  }

  def send(info: MsgInfo, why: String) {
    // lookup in network map
    // r,s  r,k  k r,s+ r,s-
    // r*,s r*,k prefer r but any ok 
    val ring = info.dest get "ring" match {
      case Some(ringName: String) => ringName
      case None => defaultRing // TODO random selection???
    }
    val nodeMap = info.dest get "node" match {
      case Some(nodeName: String) => databaseMap.rings(ring).tables(info.tab).nodes(nodeName) 
      case None => {
        //if (info.key != "null") {
          val less = info.kind.endsWith("-")
          //val nodeInfo = networkMap.getForKey(ring, info.tab, info.key, less)
          // TODO deal with node1
          val (node1,node2) = databaseMap.get(ring, info.tab, info.key, less)
          node2
          /*
          nodeInfo match {
            case Some(ni) => {
              ni.name
            }
            case None => {
              sendClient(info, "bad table", info.tab)
              return
            }
          }
          */
        //} else {
        //  "s1" // TODO error?
        //}
      }
    }
    val now: Long = System.currentTimeMillis
    val act = new MsgAction(ring, nodeMap, info.tab, now, why)
    info.history = act :: info.history
    val when = now + (timeout1 * info.history.size)
    addEvent(when, info.uid)
    sendDirect(info.kind, ring, nodeMap, self, info.uid, info.tab, info.key, info.value)
  }

  def receive = {
    case ("start") => {
      sender ! Codes.Ok
    }
    case ("stop") => {
      for ((msgId, msgInfo) <- msgs) {
        println("Shutdown error /" + config.name + " " + msgInfo)

      }
      sender ! Codes.Ok
    }
    case (kind: String, ring: String, client: Any, tab: String, key: String, value: Any) => {
      // client => server
      implicit val timeout = Timeout(5 seconds)
      val uid = uidGen.get
      val dest = if (ring == "") {
        noDest
      } else {
        Map[String,String]("ring"->ring)
      }
      // client is either ActorRef or Promise[Any] or ""
      val info = new MsgInfo(kind, dest, client, uid, tab, key, value)
      msgs = msgs + (uid -> info)
      send(info, "init")
    }
    case (kind: String, dest: Map[String, String], client: Any, tab: String, key: String, value: Any) => {
      // TODO old form, remove after deal with calls to specific node
      // client => server
      implicit val timeout = Timeout(5 seconds)
      val uid = uidGen.get
      // client is either ActorRef or Promise[Any] or ""
      val info = new MsgInfo(kind, dest, client, uid, tab, key, value)
      msgs = msgs + (uid -> info)
      send(info, "init")
    }
    case (kind: String, uid: Long, response: String) => {
      // server => client
      msgs get uid match {
        case Some(info: MsgInfo) => {
          if (kind == Codes.Handoff) {
            // TODO delay on handoff to same server or when in gap
            val range = Json(response)
            val low = jgetString(range, "low")
            val high = jgetString(range, "high")
            val lastEvent = info.history.head
            //println("handoff:"+lastEvent.ring+":"+lastEvent.server+" ["+low+","+high+"]")
            //networkMap.setRange(lastEvent.ring, lastEvent.node, lastEvent.table, low, high)
            databaseMap.setLowHigh(lastEvent.ring, lastEvent.nodeMap.nodeName, lastEvent.table, low, high)
            //val newUid = Await.result(uidGen ? "get",5 seconds).asInstanceOf[Long]
            val newUid = uidGen.get
            val newInfo = new MsgInfo(info.kind, info.dest, info.client, newUid, info.tab, info.key, info.value)
            msgs = msgs - uid
            msgs = msgs + (newUid -> newInfo)
            send(newInfo, "handoff")
          } else if (kind == Codes.Next) {
            // retry with new key
            //val newUid = Await.result(uidGen ? "get",5 seconds).asInstanceOf[Long]
            val newUid = uidGen.get
            val newInfo = new MsgInfo("next", info.dest, info.client, newUid, info.tab, response, info.value)
            msgs = msgs - uid
            msgs = msgs + (newUid -> newInfo)
            send(newInfo, "next")
          } else if (kind == Codes.PrevM) {
            // retry with new key
            // val newUid = Await.result(uidGen ? "get",5 seconds).asInstanceOf[Long]
            val newUid = uidGen.get
            val newInfo = new MsgInfo("prev-", info.dest, info.client, newUid, info.tab, response, info.value)
            msgs = msgs - uid
            msgs = msgs + (newUid -> newInfo)
            send(newInfo, "prev")
          } else {
            sendClient(info, kind, response)
          }
        }
        case None => // already processed, ignore 
      }
    }
    case ("timer") => {
      val now: Long = System.currentTimeMillis
      var done = false
      while (!done) {
        val when = events.keySet.firstKey
        if (when > now) {
          done = true
        } else {
          val uid = events.apply(when)
          events = events - uid
          msgs.get(uid) match {
            case Some(info: MsgInfo) => {
              // TODO limit number of retrys??
              send(info, "retry")
            }
            case None => // already processed
          }
        }
      }
      setTimer
    }
    case x: Any => {
      println("BAD SEND:" + x)
    }
  }

}
