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
import java.util.Map
import net.kotek.jdbm.DB
import java.io.File
import net.kotek.jdbm.DBMaker
import java.util.SortedMap
import akka.util.duration._
import akka.actor.Props
import akka.pattern._
import akka.util.Timeout
import akka.dispatch.Await
import akka.actor.ActorContext

private[persist] class StoreCommit(db: DB) extends CheckedActor {
  val system = context.system
  val maxCnt = 500
  val maxTime: Long = 1000 // 1 second

  var cnt = 0
  var lastCommit: Long = 0
  var timerRunning = false
  var done = false

  def commit(t: Long,why:String) {
    //println("Commit at: " + t +":" + why)
    db.commit()
    cnt = 0
    lastCommit = t
  }
  def rec = {
    case "start" => {
      sender ! Codes.Ok
    }
    case "done" => {
      val t = System.currentTimeMillis()
      commit(t, "done")
      done = true
      sender ! Codes.Ok
    }
    case "commit" => {
      val t = System.currentTimeMillis()
      cnt += 1
      if (cnt >= maxCnt) {
        commit(t, "count")
      } else {
        if (!timerRunning) {
          system.scheduler.scheduleOnce(maxTime milliseconds, self, "timer")
          timerRunning = true
        }
      }
    }
    case "timer" => {
      timerRunning = false
      if (!done) {
        val t = System.currentTimeMillis()
        if (t > lastCommit + maxTime) {
          commit(t, "timer")
        } else {
          system.scheduler.scheduleOnce(maxTime - (t - lastCommit) milliseconds, self, "timer")
          timerRunning = true
        }
      }
    }
  }
}

private[persist] class Store(context:ActorContext, nodeName: String, fname: String, val create: Boolean) {
  val cacheSize = 1024 * 1024 * 1 // 1 M records
  val dbfname = fixName(fname)

  if (create) {
    val f = new File(dbfname)
    if (f.exists()) {
      // remove old files
      for (f1 <- f.listFiles()) {
        f1.delete()
      }
      f.delete()
    }
    // create directory
    f.mkdirs()
  }
  val db: DB = DBMaker.openFile(dbfname + "/" + fixName(nodeName)).setMRUCacheSize(cacheSize).make()

  implicit val timeout = Timeout(5 seconds)
  val commit = context.actorOf(Props(new StoreCommit(db)), name = "@commit")
  val f = commit ? "start"
  Await.result(f, 20 seconds)

  private def fixName(s: String): String = {
    // Some file systems don't support case sensitive names
    var b = new StringBuilder()
    for (ch <- s) {
      if (ch.isUpper) {
        b += '_'
        b += ch.toLower
      } else {
        b += ch
      }
    }
    b.toString()
  }

  private def doCommit {
    commit ! "commit"
  }

  def getTable(tableName: String) = {
    val tableName1 = fixName(tableName)
    val mname = tableName1 + ":" + "meta"
    val vname = tableName1 + ":" + "vals"
    var meta0: SortedMap[String, String] = db.getTreeMap[String, String](mname)
    val meta: SortedMap[String, String] = if (meta0 == null) {
      val m = db.createTreeMap[String, String](mname)
      db.commit()
      m
    } else {
      meta0
    }
    val vals0: Map[String, String] = db.getHashMap[String, String](vname)
    val vals: Map[String, String] = if (vals0 == null) {
      val v = db.createHashMap[String, String](vname)
      db.commit()
      v
    } else {
      vals0
    }
    new StoreTable(tableName, mname, vname, context.system, this, meta, vals, doCommit)
  }

  def close() {
    val f = commit ? "done"
    Await.result(f, 1 seconds)
    db.close()
  }

  def commitChanges() {
    db.commit()
  }

  def deleteCollection(name: String) {
    db.deleteCollection(name)
  }

}