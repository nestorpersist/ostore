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

import JsonOps._
import JsonKeys._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import Stores._
import Codes.emptyResponse
import akka.dispatch.DefaultPromise
import akka.util.duration._

private[persist] case class RingCopyTasks(send: ActorRef, tableName: String, groupId: Long, toRingName: String, progress: Progress) {
  var sent:Int = 0
  private[this] var cnt: Int = 0
  def setCnt(cnt: Int) {
    this.cnt = cnt
    if (cnt == 0) done
  }
  def done() {
    cnt -= 1
    if (cnt == 0) {
      send ! (1, "endgroup", tableName, groupId)
    }
  }
}

private[persist] case class RingCopyTask(val low: String, var high: String, tasks: RingCopyTasks) {
  val high1 = high
  println(Compact(keyDecode(low)) + ":" + Compact(keyDecode(high)))
  def done() {
    tasks.done()
  }
}

private[persist] trait ServerTableBackgroundComponent { this: ServerTableAssembly =>
  val back: ServerTableBackground
  class ServerTableBackground(system:ActorSystem) {

    private[this] var tokenSent = false
    private[this] var load: Int = 50
    var qcnt = 0

    private[this] final val fastOption = Compact(JsonObject("fast" -> true))

    private[this] var tasks = List[RingCopyTask]()
    
    private type Action = Int
    private[this] final val DONE = 0
    private[this] final val THROTTLED = 1
    private[this] final val MORE = 2
    
    private[this] final val minDelay = 10
    private[this] final val maxDelay = 2000
    private[this] final var delay = minDelay
    
    private[this] final val maxInFlight = 40

    private def doCopy(task: RingCopyTask, key: String):Action = {
      val inFlight = task.tasks.sent - task.tasks.progress.cnt
      if (inFlight > maxInFlight) {
        THROTTLED
      } else {
        val meta = info.storeTable.getMeta(key) match {
          case Some(m: String) => m
          case None => "{}"
        }
        val v = info.storeTable.get(key) match {
          case Some(v1: String) => v1
          case None => NOVAL
        }
        sync.toRing(task.tasks.toRingName, key, meta, v, task.tasks.groupId, fastOption)
        task.tasks.sent += 1
        MORE
      }
    }

    private def ringCopy(task: RingCopyTask): Action = {
      val key = info.storeTable.prev(task.high, false)
      key match {
        case Some(s) => {
          if (s >= task.low) {
            val act = doCopy(task, s)
            if (act != THROTTLED) task.high = s
            act
          } else {
            DONE
          }
        }
        case None => DONE
      }
    }

    def ringCopyTask(ringName: String, nodeRef:ActorRef) = {
      val progress = new Progress
      val groupId = info.uidGen.get
      val ringTasks = new RingCopyTasks(info.send, info.tableName, groupId, ringName, progress)
      info.send ! (1, "startcopygroup", info.ringName, info.nodeName, info.tableName, groupId, nodeRef, ringName, progress)
      if (info.low == info.high) {
        // Nothing here
        ringTasks.setCnt(0)
      } else if (info.low < info.high) {
        ringTasks.setCnt(1)
        val task = new RingCopyTask(info.low, info.high, ringTasks)
        tasks = task +: tasks
      } else {
        ringTasks.setCnt(2)
        val task1 = new RingCopyTask("", info.high, ringTasks)
        tasks = task1 +: tasks
        val task2 = new RingCopyTask(info.low, "\uFFFF", ringTasks)
        tasks = task2 +: tasks
      }
    }

    private def ringTask(task: RingCopyTask, cnt: Int): Action = {
      for (i <- 0 until cnt) {
        val act = ringCopy(task)
        if (act != MORE) {
          if (act == DONE) {
            task.done()
            tasks = tasks.filter(t => t != task)
          }
          return act
        }
      }
      MORE
    }

    private def ringTasks(cnt: Int): Action = {
      var act = DONE
      for (task <- tasks) {
        val act1 = ringTask(task, cnt)
        act = math.max(act,act1)
      }
      act
    }

    def ringCopyActive(ringName: String): Boolean = {
      for (task <- tasks) {
        if (task.tasks.toRingName == ringName) return true
      }
      false
    }

    def canSendBalance(key: String): Boolean = {
      for (task <- tasks) {
        if (task.low <= key && key < task.high) return false
      }
      true
    }

    def act(self: ActorRef) = {
      val act = ringTasks(1)
      if (bal.canSend) bal.sendToNext
      if (bal.canReport) bal.reportToPrev
      // send info to monitor
      if (!tokenSent) {
        if (act != DONE || qcnt > 1) {
          val t = System.currentTimeMillis()
          //println("token:" + self + ":" + qcnt + ":" + more)
          if (act == MORE || qcnt > 1) {
            qcnt = 0
            self ! ("token", t)
            delay = minDelay
          } else {
            println("DELAY: "+ delay)
            system.scheduler.scheduleOnce(delay milliseconds, self, ("token", t))
            if (delay < maxDelay) delay = 2 * delay
          }
          tokenSent = true
        } else {
          //println("token idle:" + self)
        }
      }
    }

    def token(t: Long) = {
      val t1 = System.currentTimeMillis()
      val delta = t1 - t
      // set and use load (based on cnt and t)
      
      tokenSent = false
    }
  }
}