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

//import scala.collection.immutable.TreeMap
import java.util.SortedMap
import java.util.Map
import akka.dispatch.ExecutionContext
import akka.util.Timeout
import akka.util.duration._
import akka.actor.ActorSystem
import JsonOps._

private class JObject(var j:Json) extends java.io.Serializable {
  def writeObject(out:java.io.ObjectOutputStream) {
    out.writeObject(Compact(j))
    out.close()
    
  }
  def readObject(in:java.io.ObjectInputStream) {
    j = Json(in.readObject().asInstanceOf[String])
    in.close()
  }
  override def toString() = {
    Pretty(j)
  }
}

private[persist] class StoreTable( name:String
                                 , system: ActorSystem
                                 , store: AbstractStore
                                 , meta: SortedMap[String, String]
                                 , vals: Map[String, String]
                                 , doCommit: => Unit
                                 ) {

  lazy implicit private val ec = ExecutionContext.defaultExecutionContext(system)
  implicit private val timeout = Timeout(5 seconds)

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

  def putMeta(key: String, metav: String) {
    meta.put(key, metav)
    store.commitChanges()
  }

  def put(key: String, value: String) {
    vals.put(key, value)
    store.commitChanges()
  }

  def putBoth(key: String, metav: String, value: String) {
    meta.put(key, metav)
    vals.put(key, value)
    store.commitChanges()
  }

  /*
  def putBothF(key: String, metav: String, value: String, fast: Boolean): Future[Unit] = {
    // Always update the in memory data
    val f = Future[Unit] {
      meta.put(key, metav)
      vals.put(key, value)
    }
    if (fast) {
      // if fast disk update can happen asynchronously
      f map { x =>
        db.commit()
      }
      f
    } else {
      // if not fast also update disk data
      f map { x =>
        db.commit()
      }
    }
  }
  */
  
  def putBothF1(key: String, metav: String, value: String, fast: Boolean) {
    // TODO the following 2 stmts need to be atomic
    vals.put(key, value)
    meta.put(key, metav)
    if (fast) {
      // delay and batch up commits
      doCommit
    } else {
      store.commitChanges()
    }
  }

  def remove(key: String) {
    meta.remove(key)
    vals.remove(key)
    store.commitChanges()
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

  def close() = {
    //println(vals.keySet())
    //db.close()
  }
  
  def delete() = {
    store.deleteCollection(name)
  }

  override def toString(): String = {
    meta.toString()
  }
}