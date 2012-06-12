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

import akka.dispatch.Future
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.dispatch.DefaultPromise
import JsonOps._

trait NextItem {
  val value: Json
  def next(): Future[Option[NextItem]]
}

class ForwardNextItem(val value: Json, key: String,
  tableName: String, equal: Boolean, options: String, high: String, includeHigh: Boolean, parent: JsonKey,
  send: ActorRef, system: ActorSystem) extends NextItem {

  implicit val executor = system.dispatcher

  def next(): Future[Option[NextItem]] = {
    val cmd = if (equal) "next" else "next+"
    val dest = Map("ring" -> "r1")
    var f1 = new DefaultPromise[Any]
    send ! (cmd, dest, f1, tableName, key, options)
    f1.map(x => { tryNext(x) })
  }

  private def tryNext(x: Any): Option[NextItem] = {
    val (code: String, result: String) = x
    if (code == Codes.Done) {
      None
    } else {
      val jresult = Json(result)
      val key = keyEncode(jresult match {
        case j:JsonObject => jgetString(j,"k")
        case j => j
      })
      if (key > high) {
        None
      } else if ((!includeHigh) && key == high) {
        None
      } else {
        Some(doNext(key, result))
      }
    }
  }

  private def doNext(key: String, result: String): NextItem = {
    val (newValue, newKey) = parent match {
      case parenta: JsonArray => {
        val jkey = keyDecode(key)
        jkey match {
          case jkeya: JsonArray => {
            val jkey1 = if (jsize(jkeya) == jsize(parenta)) {
              parenta
            } else {
              var jkey2 = JsonArray()
              var i = 0
              for (elem <- jkeya) {
                if (i <= jsize(parenta)) {
                  jkey2 = elem +: jkey2
                }
                i += 1
              }
              jkey2.reverse
            }
            (jkey1, keyEncode(jkey1) + "\uFFFF")
          }
          case x => throw new Exception("internal error: key not array")
        }
      }
      case x => (Json(result), key)
    }
    new ForwardNextItem(newValue, newKey,
      tableName, false, options, high, includeHigh, parent,
      send, system)
  }
}

class BackwardNextItem(val value: Json, key: String,
  tableName: String, equal: Boolean, options: String, low: String, includeLow: Boolean, parent: JsonKey,
  send: ActorRef, system: ActorSystem) extends NextItem {

  implicit val executor = system.dispatcher

  def next(): Future[Option[NextItem]] = {
    val cmd = if (equal) "prev" else "prev-"
    val dest = Map("ring" -> "r1")
    var f1 = new DefaultPromise[Any]
    send ! (cmd, dest, f1, tableName, key, options)
    f1.map(x => { tryNext(x) })
  }

  private def tryNext(x: Any): Option[NextItem] = {
    val (code: String, result: String) = x
    if (code == Codes.Done) {
      None
    } else {
      val jresult = Json(result)
      val key = keyEncode(jresult match {
        case j:JsonObject => jgetString(j,"k")
        case j => j
      })
      if (key < low) {
        None
      } else if ((!includeLow) && key == low) {
        None
      } else {
        Some(doNext(key, result))
      }
    }
  }

  private def doNext(key: String, result: String): NextItem = {
    val (newValue, newKey) = parent match { //if (parent.isArray()) {
      case parenta:JsonArray => {
        val jkey = keyDecode(key)
        jkey match {
          case jkeya:JsonArray => {
            val jkey1 = if (jsize(jkeya) == jsize(parenta)) {
              jkey
            } else {
               var jkey2 = JsonArray()
               var i = 0
               for (elem<-jkeya) {
                 if (i <= jsize(parenta)) {
                   jkey2 = elem +: jkey2
                 }
                 i += 1
               }
               jkey2.reverse
            }
            (jkey1, keyEncode(jkey1))
          }
          case x => throw new Exception("internal error: key not array")
        }
      }
      case x => (Json(result), key)
    }
    new BackwardNextItem(newValue, newKey,
      tableName, false, options, low, includeLow, parent,
      send, system)
  }
}

