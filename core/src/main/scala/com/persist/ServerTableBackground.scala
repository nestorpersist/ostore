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

import com.persist.JsonOps._
import akka.actor.ActorRef

private[persist] case class RingCopyTask(val ringName: String, val low: String, var high: String)

private[persist] trait ServerTableBackgroundComponent { this: ServerTableAssembly =>
  val back: ServerTableBackground
  class ServerTableBackground {

    private var tokenSent = false
    private var load: Int = 50
    var qcnt = 0

    private var tasks = List[RingCopyTask]()

    private def doCopy(ringName: String, key: String) {
      //println("Copy:" + info.tableName + ":" + Compact(keyDecode(key)) + " from " + info.ringName + "/" + info.nodeName + " to " + ringName)
      val meta = info.storeTable.getMeta(key) match {
        case Some(m: String) => m
        case None => "{}"
      }
      val v = info.storeTable.get(key) match {
        case Some(v1: String) => v1
        case None => "null"
      }
      sync.toRing(ringName, key, meta, v)
    }

    private def ringCopy(task: RingCopyTask): Boolean = {
      val key = info.storeTable.prev(task.high, false)
      key match {
        case Some(s) => {
          if (s >= task.low) {
            doCopy(task.ringName, s)
            task.high = s
            true
          } else {
            false
          }
        }
        case None => false
      }
    }

    def ringCopyTask(ringName: String) = {
      if (info.low == info.high) {
        // Nothing here
      } else if (info.low < info.high) {
        val task = new RingCopyTask(ringName, info.low, info.high)
        tasks = task +: tasks
      } else {
        val task1 = new RingCopyTask(ringName, "", info.high)
        tasks = task1 +: tasks
        val task2 = new RingCopyTask(ringName, info.low, "\uFFFF")
        tasks = task2 +: tasks
      }
      // TODO change to incremental
      //for (task <- tasks) {
      //  while (ringCopy(task)) { }
      //}
    }

    private def ringTask(task: RingCopyTask, cnt: Int): Boolean = {
      for (i <- 0 until cnt) {
        if (ringCopy(task)) {
        } else {
          tasks = tasks.filter(t => t != task)
          return false
        }
      }
      true
    }

    private def ringTasks(cnt: Int): Boolean = {
      var more = false
      for (task <- tasks) {
        if (ringTask(task, cnt)) more = true
      }
      more
    }

    def ringCopyActive(ringName: String): Boolean = {
      for (task <- tasks) {
        if (task.ringName == ringName) return true
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
      val more = ringTasks(1)
      if (bal.canSend) bal.sendToNext
      if (bal.canReport) bal.reportToPrev
      // send info to monitor
      if (!tokenSent) {
        if (more || qcnt > 1) {
          val t = System.currentTimeMillis()
          //println("token:" + self + ":" + qcnt + ":" + more)
          qcnt = 0
          self ! ("token", t)
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