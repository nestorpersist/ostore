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
import JsonOps._
import akka.actor.ActorSystem
import Stores._
import scala.collection.immutable.TreeMap
import scala.collection.immutable.HashMap

private[persist] class InMemory extends Store {

  var store = HashMap[String, InMemoryTable]()

  def init(name: String, config: Json, context: ActorContext, create: Boolean) {
  }

  def created: Boolean = true

  def getTable(tableName: String) = {
    store.getOrElse(tableName, {
      val table = new InMemoryTable(tableName, this)
      store += (tableName -> table)
      table
    })
  }

  def close() {}

  def deleteTable(tableName: String) {
    store -= tableName
  }
}

private[persist] class InMemoryTable(val tableName: String, val store: Store)
  extends StoreTable {

  // Note: TreeMap performance improved in Scala 2.10
  private var meta = TreeMap[String, String]()
  private var vals = HashMap[String, String]()
  private var control = HashMap[String, String]()

  def next(key: String, equal: Boolean): Option[String] = {
    if (equal) {
      meta.from(key).keys.headOption
    } else {
      meta.from(key +"\u0000").keys.headOption
    }
  }

  def prev(key: String, equal: Boolean): Option[String] = {
    val meta1 = if (equal) {
      meta.to(key)
    } else {
      meta.until(key)
    }
    if (meta1.size == 0) {
      None
    } else {
      Some(meta1.lastKey)
    }
  }

  def size(): Long = {
    meta.size
  }

  def vsize(): Long = {
    vals.size
  }

  def getMeta(key: String): Option[String] = {
    meta.get(key)
  }

  def get(key: String): Option[String] = {
    vals.get(key)
  }

  def getControl(key: String): Option[String] = {
    control.get(key)
  }

  def put(key: String, value: String) {
    vals += (key -> value)
  }

  def putControl(key: String, value: String) {
    control += (key -> value)
  }

  def put(key: String, metav: String, value: Option[String], fast: Boolean) {
    value match {
      case None => vals -= key
      case Some(v) => vals += (key -> v)
    }
    meta += (key -> metav)
  }

  def remove(key: String) {
    meta -= key
    vals -= key
  }

  def removeControl(key: String) {
    control -= key
  }

  def first(): Option[String] = {
    meta.keys.headOption
  }

  def last(): Option[String] = {
    if (meta.size == 0) {
      None
    } else {
      Some(meta.lastKey)
    }
  }

  def close() = { }
  
  override def toString(): String = {
    meta.toString()
  }
}
