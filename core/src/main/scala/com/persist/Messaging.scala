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
import JsonKeys._
import scala.collection.immutable.TreeMap
import scala.collection.mutable.HashMap
import akka.actor.Cancellable
import akka.pattern._
import akka.util.Timeout
import akka.util.duration._
import akka.actor.ActorRef
import akka.event.Logging
import Exceptions._
import akka.actor.ActorLogging
import akka.dispatch.Promise
import Codes.emptyResponse
import akka.actor.Props

private[persist] class Progress {
  private[this] var cnt1:Long = 0
  def cnt:Long = {
    synchronized { cnt1 }
  }
  def cnt_=(c:Long) { 
    synchronized{ cnt1 = c }
  }
}

private[persist] class Messaging(config: DatabaseConfig, client: Option[Client]) extends CheckedActor {

  private[this] val system = context.system
  private[this] val map = DatabaseMap(config)
  private[this] val msgs = new Msgs()
  private[this] val groups = new Groups()
  private[this] val databaseName = config.name
  private[this] val change = context.actorOf(Props(new Change(config, client, self)), name = "change")
  private[this] var serverMsgCount: Long = 0L

  private trait Group {

    def addCommand(cmd: String): Unit
    def ready(): Unit
    def endCommand(cmd: String): Unit
    def send(): Boolean
  }

  private class PutGroup(
    sender: ActorRef,
    uid: Long,
    w: Int) extends Group {
    private[this] var needSync = 0
    private[this] var needOther = 0
    private[this] var sync = 0
    private[this] var other = 0
    private[this] var isReady = false
    def addCommand(cmd: String) {
      if (cmd == "sync") {
        if (needSync < w) needSync += 1
      } else {
        needOther += 1
      }
    }
    def ready() {
      isReady = true
    }
    def endCommand(cmd: String) {
      if (cmd == "sync") sync += 1 else other += 1
    }
    def send(): Boolean = {
      if (isReady && sync >= needSync && other >= needOther) {
        sender ! (Codes.Ok, uid, emptyResponse)
        true
      } else {
        false
      }
    }
  }

  private class CopyGroup(
    nodeRef: ActorRef,
    ringName: String,
    nodeName: String,
    tableName: String,
    toRingName: String,
    progress:Progress) extends Group {
    private[this] var needOther = 0
    private[this] var other = 0
    private[this] var isReady = false
    private[this] var high:Int = 0
    def addCommand(cmd: String) {
      needOther += 1
    }
    def ready() {
      isReady = true
    }
    def endCommand(cmd: String) {
      other += 1
      progress.cnt = other
    }
    def send(): Boolean = {
      if (isReady && other >= needOther) {
        nodeRef ! ("nodeCopyAct", toRingName, tableName)
        //println("MSGRINGCOPY:" + " /" + ringName + "/" + nodeName + "/" + tableName + " => " + toRingName + " " + other)
        true
      } else {
        false
      }
    }
  }

  private class Groups {
    // TODO time out groups (2 minutes???)
    private[this] val groups = HashMap[String, HashMap[Long, Group]]()

    def addGroup(table: String, id: Long, group:Group) { 
      val subgroup = if (groups.contains(table)) {
        groups(table)
      } else {
        val subgroup1 = new HashMap[Long, Group]()
        groups += (table -> subgroup1)
        subgroup1
      }
      subgroup += (id -> group)
    }

    private def removeGroup(table: String, id: Long) {
      if (groups.contains(table)) {
        val subgroup = groups(table)
        if (subgroup.contains(id)) {
          subgroup -= id
        }
      }
    }

    private def getGroup(table: String, id: Long): Option[Group] = {
      if (groups.contains(table)) {
        val subgroup = groups(table)
        subgroup.get(id)
      } else {
        None
      }
    }

    def ready(table: String, id: Long) = {
      getGroup(table, id) match {
        case Some(g: Group) => {
          g.ready()
          if (g.send()) removeGroup(table, id)
        }
        case None =>
      }
    }

    def addCommand(table: String, id: Long, cmd: String) {
      getGroup(table, id) match {
        case Some(g: Group) => {
          g.addCommand(cmd)
        }
        case None =>
      }
    }

    def endCommand(table: String, id: Long, cmd: String) {
      getGroup(table, id) match {
        case Some(g: Group) => {
          g.endCommand(cmd)
          if (g.send()) removeGroup(table, id)
        }
        case None =>
      }
    }
  }

  private class Msg(
    var cmd: String,
    val ringName: String,
    val tableName: String,
    var key: String,
    val value: Any) {

    var actualRingName: String = ""
    var uid: Long = 0L
    var nodeName = ""
    var event: Long = 0L
    var retryCount: Int = 0 // number of retries
    var count: Int = 0 // number of different uid's used

    private def report(): Boolean = {
      retryCount > 0 && (retryCount % 10) == 0
    }

    def toJson(): JsonObject = {
      JsonObject("database" -> databaseName, "ring" -> ringName,
        "node" -> nodeName, "table" -> tableName, "key" -> keyDecode(key), "cmd" -> cmd)
    }

    def send() {
      val less = cmd.endsWith("-")
      val info = map.get(ringName, tableName, key, less)
      actualRingName = info.node.ringName
      val ref = info.getRef(system)
      nodeName = info.node.nodeName
      if (report()) {
        val info = toJson() + ("msg" -> "retry", "cnt" -> retryCount)
        log.warning(Compact(info))
      }
      retryCount += 1
      ref ! (cmd, uid, key, value)
    }
  }

  private class ServerMsg(
    cmd: String,
    ringName: String,
    tableName: String,
    key: String,
    value: Any,
    val id: Long) extends Msg(cmd, ringName, tableName, key, value)

  private class ClientMsg(
    cmd: String,
    ringName: String,
    tableName: String,
    key: String,
    value: Any,
    val reply: Promise[Any])
    extends Msg(cmd, ringName, tableName, key, value)

  private class Msgs() {
    private[this] val uidGen = new UidGen

    private[this] var msgs = TreeMap[Long, Msg]()
    private[this] var events = TreeMap[Long, Long]()

    private[this] var timer: Cancellable = null
    private[this] var timerWhen = 0L

    def size = msgs.size

    private def timeout(cnt: Int, delay: Boolean): Int = {
      if (cnt < 5) {
        if (delay) 100 else 1000
      } else if (cnt < 20) {
        100
      } else {
        20000
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
          timer = system.scheduler.scheduleOnce(delta milliseconds, self, ("timer"))
          timerWhen = when
        } else {
          self ! ("timer")
        }
      }
    }

    private def addEvent(msg: Msg, delay: Boolean) {
      val now = System.currentTimeMillis()
      val uid = msg.uid
      val when = now + timeout(msg.retryCount, delay)
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
      msg match {
        case cm: ClientMsg => {
          if (msg.retryCount > 3) {
            cm.reply.success((Codes.Timeout, emptyResponse))
            return
          }
        }
        case _ =>
      }
      if (!map.hasRing(msg.ringName)) {
        change ! ("addRing", msg.uid, msg.ringName)
        return
      }
      if (!map.hasTable(msg.tableName)) {
        change ! ("addTable", msg.uid, msg.tableName)
        return
      }
      addEvent(msg, delay)
      if (delay) {
        // delay until timeout
        // this helps with items being moved by balance
      } else {
        msg.send()
      }
    }

    def newUid(msg: Msg) {
      val uid = uidGen.get
      msg.uid = uid
      msg.retryCount = 0
      msg.count += 1
      msgs += (uid -> msg)
    }

    def newMsg(msg: Msg, delay: Boolean = false) {
      newUid(msg)
      send(msg, delay)
    }

    def get(uid: Long): Option[Msg] = {
      msgs.get(uid)
    }

    def delete(uid: Long) {
      get(uid) match {
        case Some(msg) => {
          events -= msg.event
          msgs -= uid
          msg.uid = 0L
        }
        case None =>
      }
    }

    def timerAction {
      timer = null
      val now: Long = System.currentTimeMillis
      var done = false
      while (!done) {
        val when = if (events.keySet.size == 0) now + 1 else events.keySet.firstKey
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

  def handleResponse(code: String, response: String, msg: Msg) {
    msgs.delete(msg.uid)
    code match {
      case Codes.Handoff => {
        val jresponse = Json(response)
        val low = jgetString(jresponse, "low")
        val high = jgetString(jresponse, "high")
        val nextNodeName = jgetString(jresponse, "next")
        //val nextHost = jgetString(jresponse, "host")
        //val nextPort = jgetInt(jresponse, "port")
        val changed = map.setLowHigh(msg.actualRingName, msg.nodeName, msg.tableName, low, high)
        /*
        if (! map.hasRing(msg.actualRingName)) {
          msgs.newUid(msg)
          change ! ("addRing", msg.uid, msg.actualRingName) 
          return
        }
        */
        if (!map.hasNode(msg.actualRingName, nextNodeName)) {
          msgs.newUid(msg)
          change ! ("addNode", msg.uid, msg.actualRingName, nextNodeName)
          return
        }
        msgs.newMsg(msg, delay = !changed)
      }
      case Codes.Next => {
        // retry with new key
        msg.cmd = "next"
        msg.key = response
        msgs.newMsg(msg)
      }
      case Codes.PrevM => {
        // retry with new key
        msg.cmd = "prev-"
        msg.key = response
        msgs.newMsg(msg)
      }
      case _ => {
        msg match {
          case cm: ClientMsg => {
            if (code == Codes.NoRing) {
              msgs.newUid(msg)
              val rings = map.allRings()
              change ! ("deleteRing", msg.uid, msg.actualRingName, rings)
            } else if (code == Codes.NoNode) {
              msgs.newUid(msg)
              change ! ("deleteNode", msg.uid, msg.actualRingName, msg.nodeName)
            } else {
              cm.reply.success((code, response))
            }
          }
          case sm: ServerMsg => {
            if (code == Codes.Ok) {
              if (sm.id != 0) groups.endCommand(sm.tableName, sm.id, sm.cmd)
              // done
            } else {
              val info = msg.toJson() + ("msg" -> "failing server message", "code" -> code)
              throw new SystemException(Codes.InternalError, info)
            }
          }
        }
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
    case ("busy") => {
      sender ! (Codes.Ok, serverMsgCount, msgs.size)
    }
    case (cmd: String, ringName: String, id: Long, tableName: String, key: String, value: Any) => {
      // server request
      // TODO value should be String so msgs can be stored in persist store
      if (id != 0) {
        groups.addCommand(tableName, id, cmd)
      }
      serverMsgCount += 1
      val msg = new ServerMsg(cmd, ringName, tableName, key, value, id)
      msgs.newMsg(msg)
    }
    case (cmd: String, ringName: String, reply: Promise[Any], tableName: String, key: String, value: Any) => {
      // client request
      val msg = new ClientMsg(cmd, ringName, tableName, key, value, reply)
      msgs.newMsg(msg)
    }
    case (1, "startgroup", tableName: String, id: Long, sender: ActorRef, uid: Long, syncCnt: Int) => {
      val group = new PutGroup(sender, uid, syncCnt)
      groups.addGroup(tableName, id, group)
    }
    case (1, "startcopygroup", ringName:String, nodeName:String, tableName:String, id:Long, nodeRef:ActorRef, toRingName:String, progress:Progress) => {
      val group = new CopyGroup(nodeRef, ringName, nodeName, tableName, toRingName, progress)
      groups.addGroup(tableName, id, group)
    }
    case (1, "endgroup", tableName: String, id: Long) => {
      groups.ready(tableName, id)
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
    // TODO r=n
    case (0, "addRing", ringName: String, nodes: JsonArray) => {
      if (!map.hasRing(ringName)) {
        map.addRing(ringName, nodes)
      }
      sender ! Codes.Ok
    }
    case (0, "deleteRing", ringName: String) => {
      if (map.hasRing(ringName)) {
        map.deleteRing(ringName)
      }
      sender ! Codes.Ok
    }
    case (0, "addTable", tableName: String) => {
      if (!map.hasTable(tableName)) {
        map.addTable(tableName)
      }
      sender ! Codes.Ok
    }
    case (0, "deleteTable", tableName: String) => {
      if (map.hasTable(tableName)) {
        map.deleteTable(tableName)
      }
      sender ! Codes.Ok
    }
    case (0, "addNode", ringName: String, prevNodeName: String, newNodeName: String, host: String, port: Int) => {
      map.addNode(ringName, prevNodeName, newNodeName, host, port)
      sender ! Codes.Ok
    }
    case (0, "deleteNode", ringName: String, nodeName: String) => {
      map.deleteNode(ringName, nodeName)
      sender ! Codes.Ok
    }
    case ("continue", uid: Long) => {
      msgs.get(uid) match {
        case Some(msg) => {
          msgs.delete(msg.uid)
          msgs.newMsg(msg)
        }
        case None => // ignore
      }
    }
    case ("fail", uid: Long, code: String, response: JsonObject) => {
      msgs.get(uid) match {
        case Some(msg: ServerMsg) => {
          msgs.delete(msg.uid)
          log.error("Server messaging fail:" + code + ":" + Compact(response))
        }
        case Some(msg: ClientMsg) => {
          msgs.delete(msg.uid)
          msg.reply.success((code, Compact(response)))
        }
        case None => // ignore
      }
    }
    case ("timer") => {
      msgs.timerAction
    }
  }
}