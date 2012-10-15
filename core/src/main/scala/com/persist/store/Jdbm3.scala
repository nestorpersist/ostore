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

package com.persist.store

import com.persist._
import akka.actor.ActorContext
import akka.actor.ActorRef
import java.io.File
import net.kotek.jdbm.DBMaker
import net.kotek.jdbm.DB
import akka.util.Timeout
import akka.util.duration._
import akka.actor.Props
import akka.pattern._
import akka.dispatch.Await
import java.util.SortedMap
import java.util.Map
import JsonOps._
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import Stores._
import akka.actor.Actor

private[persist] class StoreCommit(db: DB) extends CheckedActor {
  val system = context.system
  val maxCnt = 500
  val maxTime: Long = 1000 // 1 second

  var cnt = 0
  var lastCommit: Long = 0
  var timerRunning = false
  var done = false

  def commit(t: Long, why: String) {
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

private[persist] class Jdbm3 extends Store {

  private var create = false
  private var db: DB = null
  private var commit: ActorRef = null
  private implicit val timeout = Timeout(5 seconds)

  def init(name: String, config: Json, context: ActorContext, create1: Boolean) {
    val path = jgetString(config, "path")
    val fname = path + "/" + name
    val dbfname = fixName(fname)
    val file = new File(dbfname)
    val exists = file.exists()
    create = !exists || create1
    if (create) {
      if (exists) {
        // remove old files
        for (f1 <- file.listFiles()) {
          f1.delete()
        }
        file.delete()
      }
      // create directory
      file.mkdirs()
    }
    val cacheSize = 1024 * 1024 * 1 // 1 M records
    db = DBMaker.openFile(dbfname + "/tables").setMRUCacheSize(cacheSize).make()
    commit = context.actorOf(Props(new StoreCommit(db)), name = "@commit")
    val f = commit ? "start"
    Await.result(f, 20 seconds)
  }

  def created = create

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
    val cname = tableName1 + ":" + "control"
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
    val control0: Map[String, String] = db.getHashMap[String, String](cname)
    val control: Map[String, String] = if (control0 == null) {
      val c = db.createHashMap[String, String](cname)
      db.commit()
      c
    } else {
      control0
    }
    new Jdbm3Table(tableName, this, meta, vals, control, doCommit, commitChanges)
  }

  def close() {
    val f = commit ? "done"
    Await.result(f, 1 seconds)
    db.close()
  }

  private def commitChanges {
    db.commit()
  }

  def deleteTable(tableName: String) {
    db.deleteCollection(tableName + ":meta")
    db.deleteCollection(tableName + ":vals")
    db.deleteCollection(tableName + ":control")
  }
}

private class JObject(var j: Json) extends java.io.Serializable {
  def writeObject(out: java.io.ObjectOutputStream) {
    out.writeObject(Compact(j))
    out.close()

  }
  def readObject(in: java.io.ObjectInputStream) {
    j = Json(in.readObject().asInstanceOf[String])
    in.close()
  }
  override def toString() = {
    Pretty(j)
  }
}

private[persist] class Jdbm3Table(val tableName: String, val store: Store,
  meta: SortedMap[String, String], vals: Map[String, String], control: Map[String, String], doCommit: => Unit, commitChanges: => Unit)
  extends StoreTable {

  def next(key: String, equal: Boolean): Option[String] = {
    val map2 = meta.tailMap(key)
    val keys = map2.keySet()
    val it = keys.iterator()
    while (it.hasNext()) {
      val key1 = it.next()
      if (equal || key1 != key) {
        return Some(key1)
      }
    }
    None
  }

  def prev(key: String, equal: Boolean): Option[String] = {
    if (equal && meta.containsKey(key)) {
      Some(key)
    } else {
      val map2 = meta.headMap(key)
      if (map2.size() > 0) {
        val key1 = map2.lastKey()
        Some(key1)
      } else {
        None
      }
    }
  }

  def size(): Long = {
    meta.size()
  }

  def vsize(): Long = {
    vals.size()
  }

  def getMeta(key: String): Option[String] = {
    val v = meta.get(key)
    if (v == null) {
      None
    } else {
      Some(v)
    }
  }

  def get(key: String): Option[String] = {
    val v = vals.get(key)
    if (v == null) {
      None
    } else {
      Some(v)
    }
  }

  def getControl(key: String): Option[String] = {
    val v = control.get(key)
    if (v == null) {
      None
    } else {
      Some(v)
    }
  }
  
  def put(key: String, value: String) {
    vals.put(key, value)
    commitChanges
  }
  
  def putControl(key: String, value: String) {
    control.put(key, value)
    commitChanges
  }

  def put(key: String, metav: String, value: Option[String], fast: Boolean) {
    // TODO the following 2 stmts need to be atomic??
    value match {
      case None => vals.remove(key)
      case Some(v) => vals.put(key, v) 
    }
    meta.put(key, metav)
    if (fast) {
      // delay and batch up commits
      doCommit
    } else {
      commitChanges
    }
  }

  def remove(key: String) {
    meta.remove(key)
    vals.remove(key)
    commitChanges
  }
  
  def removeControl(key: String) {
    control.remove(key)
  }

  def first(): Option[String] = {
    if (meta.size() == 0) {
      None
    } else {
      Some(meta.firstKey())
    }
  }

  def last(): Option[String] = {
    if (meta.size() == 0) {
      None
    } else {
      Some(meta.lastKey())
    }
  }
  
  def close() {}

  override def toString(): String = {
    meta.toString()
  }
}
