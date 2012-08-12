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
import JsonOps._
import scala.collection.immutable.TreeMap
import akka.actor.Cancellable
import akka.pattern._
import akka.util.Timeout
import akka.util.duration._
import akka.actor.ActorRef
import akka.event.Logging
import Exceptions._
import akka.actor.ActorLogging

private[persist] class Msg(val uid: Long, val cmd: String, val ringName: String, val tableName: String,
  val key: String, val value: Any, val less: Boolean) {

  var nodeName = ""
  var event: Long = 0L
  var cnt: Int = 0

  private def report(cnt: Int): Boolean = {
    (cnt % 10) == 0
  }

  def toJson: JsonObject = {
    JsonObject("ring" -> ringName, "node" -> nodeName, "table" -> tableName, "key" -> keyDecode(key), "cmd" -> cmd)
  }

  def send(messaging: Messaging) {
    val info = messaging.map.get(ringName, tableName, key, less)
    val ref = info.getRef(messaging.system)
    nodeName = info.node.nodeName
    if (report(cnt)) {
      val info = toJson + ("msg" -> "retry", "cnt" -> cnt)
      messaging.log.warning(Compact(toJson))
    }
    cnt += 1
    ref ! (cmd, uid, key, value)
  }
}

private[persist] class Msgs(messaging: Messaging) {
  private val uidGen = new UidGen

  private var msgs = TreeMap[Long, Msg]()

  private var events = TreeMap[Long, Long]()
  private var timer: Cancellable = null
  private var timerWhen = 0L

  private def timeout(cnt: Int, delay:Boolean): Int = {
    if (cnt < 5) {
      if (delay) 1 else 2
    } else if (cnt < 20) {
      10
    } else {
      20
    }
  }

  private def setTimer {
    if (events.size > 0) {
      val when = events.firstKey
      val now: Long = System.currentTimeMillis
      if (timer != null) {
        if (when >= timerWhen) return // use previously scheduled timer
        // cancel previous timer
        timer.cancel()
        timer = null
      }
      if (when > now) {
        val delta: Long = when - now
        timer = messaging.system.scheduler.scheduleOnce(delta milliseconds, messaging.self, ("timer"))
        timerWhen = when
      } else {
        messaging.self ! ("timer")
      }
    }
  }

  private def addEvent(msg: Msg, delay:Boolean) {
    val now = System.currentTimeMillis()
    val uid = msg.uid
    val when = now + timeout(msg.cnt,delay)
    var done = false
    var when1 = when
    do {
      if (events.get(when1) == None) {
        done = true
      } else {
        when1 += 1
      }
    } while (!done)
    events = events + (when1 -> uid)
    msg.event = when1
    setTimer
  }

  private def send(msg: Msg, delay: Boolean) {
    addEvent(msg, delay)
    if (delay) {
      // delay until timeout
      // this helps with items being moved by balance
    } else {
      msg.send(messaging)
    }
  }

  def newMsg(cmd: String, ringName: String, tableName: String, key: String, value: Any, less: Boolean = false, delay: Boolean = false) {
    val uid = uidGen.get
    val msg = new Msg(uid, cmd, ringName, tableName, key, value, less)
    msgs += (uid -> msg)
    send(msg, delay)
  }

  def get(uid: Long): Option[Msg] = {
    msgs.get(uid)
    None
  }

  def delete(uid: Long) {
    get(uid) match {
      case Some(msg) => {
        events -= msg.event
        msgs -= uid
      }
      case None =>
    }
  }

  def timerAction {
    val now: Long = System.currentTimeMillis
    var done = false
    while (!done) {
      val when = events.keySet.firstKey
      if (when > now) {
        done = true
      } else {
        val uid = events(when)
        events = events - when
        get(uid) match {
          case Some(msg: Msg) => {
            send(msg, false)
          }
          case None => // already processed
        }
      }
    }
    setTimer
  }
}

private[persist] class Messaging(config: DatabaseConfig) extends CheckedActor with ActorLogging {

  val system = context.system
  val map = DatabaseMap(config)
  val msgs = new Msgs(this)

  def handleResponse(code: String, response: String, msg: Msg) {
    code match {
      case Codes.Ok => {
        msgs.delete(msg.uid)
      }
      case Codes.Handoff => {
        val jresponse = Json(response)
        val low = jgetString(jresponse, "low")
        val high = jgetString(jresponse, "high")
        //val nextNodeName = jgetString(jresponse, "next")
        //val nextHost = jgetString(jresponse, "host")
        //val nextPort = jgetInt(jresponse, "port")
        val changed = map.setLowHigh(msg.ringName, msg.nodeName, msg.tableName, low, high)
        msgs.delete(msg.uid)
        msgs.newMsg(msg.cmd, msg.ringName, msg.tableName, msg.key, msg.value, delay = !changed)
      }
      case Codes.Next => {
        // retry with new key
        msgs.delete(msg.uid)
        msgs.newMsg("next", msg.ringName, msg.tableName, response, msg.value)
      }
      case Codes.PrevM => {
        // retry with new key
        msgs.delete(msg.uid)
        msgs.newMsg("prev-", msg.ringName, msg.tableName, response, msg.value, less = true)
      }
      case code => {
        val info = msg.toJson + ("msg" -> "failing server message", "code" -> code)
        throw new SystemException(Codes.InternalError, info)
      }
    }
  }

  def rec = {
    case ("start") => {
      sender ! Codes.Ok
    }
    case ("stop") => {
      //for ((msgId, msgInfo) <- msgs) {
      //println("Shutdown error /" + config.name + " " + msgInfo)
      //}
      sender ! Codes.Ok
    }
    case (cmd: String, ringName: String, tableName: String, key: String, value: Any) => {
      // server request
      // TODO value should be String so msgs can be stored in persist store
      msgs.newMsg(cmd, ringName, tableName, key, value)
    }
    case (code: String, uid: Long, response: String) => {
      // response
      msgs.get(uid) match {
        case Some(msg) => {
          handleResponse(code, response, msg)
        }
        case None => // Discard it (already processed)
      }
    }
    // TODO check for idle
    // TODO r=n, w=n
    case ("addRing", ringName: String, nodes: JsonArray) => {
      map.addRing(ringName, nodes)
      sender ! Codes.Ok
    }
    case ("deleteRing", ringName: String) => {
      map.deleteRing(ringName)
      sender ! Codes.Ok
    }
    // TODO (add,delete)(node,table)
    case ("timer") => {
      msgs.timerAction
    }
  }
}