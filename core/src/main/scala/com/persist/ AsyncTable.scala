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

import akka.actor.Actor
import akka.dispatch.Future
import Actor._
import JsonOps._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.DefaultPromise
import akka.util.duration._
import akka.dispatch._
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout

class AsyncTable(tableName: String, system: ActorSystem, send: ActorRef) {
  // TODO pass in config rather than default to ring r1
  // TODO option to get config from remote server

  implicit val executor = system.dispatcher

  private def put1(key: JsonKey, value: Json, vectorClock: Option[Json],
    options: Json, create: Boolean): Future[Boolean] = {
    var request = JsonObject()
    vectorClock match {
      case Some(c) => request = request + ("c" -> c)
      case None =>
    }
    if (jsize(options) > 0) request = request + ("o" -> options)
    if (create) request = request + ("create" -> true)
    val dest = Map("ring" -> "r1")
    var f1 = new DefaultPromise[Any]
    send ! ("put", dest, f1, tableName, keyEncode(key), (Compact(value),Compact(request)))
    val f2 = f1.map { x =>
      {
        val (code: String, v1: String) = x
        if (code == Codes.NoPut) {
          false
        } else if (code == Codes.Ok) {
          true
        } else {
          throw new Exception(code)
        }
      }
    }
    f2
  }

  def put(key: JsonKey, value: Json, options: Json = emptyJsonObject): Future[Unit] = {
    val f1 = put1(key, value, None, options, false)
    f1.map { b => }
  }

  def update(key: JsonKey, value: Json, vectorClock: Json, options: Json = emptyJsonObject): Future[Boolean] = {
    put1(key, value, Some(vectorClock), options, false)
  }

  def create(key: JsonKey, value: Json, options: Json = emptyJsonObject): Future[Boolean] = {
    put1(key, value, None, options, true)
  }

  def delete(key: JsonKey, options: Json = emptyJsonObject): Future[Unit] = {
    val dest = Map("ring" -> "r1")
    var f1 = new DefaultPromise[Any]
    send ! ("delete", dest, f1, tableName, keyEncode(key), Compact(options))
    val f2 = f1.map { x =>
      {
        val (code: String, v1: String) = x
        if (code != Codes.Ok) {
          throw new Exception(code)
        }
      }
    }
    f2
  }

  def get(key: JsonKey, options: Json = emptyJsonObject): Future[Option[Json]] = {
    val dest = Map("ring" -> "r1")
    var f1 = new DefaultPromise[(String, String)]
    send ! ("get", dest, f1, tableName, keyEncode(key), Compact(options))
    val f2 = f1.map { x =>
      {
        val (code: String, v1: String) = x
        if (code == Codes.Ok) {
          Some(Json(v1))
        } else if (code == Codes.NotPresent) {
          None
        } else {
          throw new Exception(code)
        }
      }
    }
    f2
  }

  def all(options: Json = emptyJsonObject): Future[Option[NextItem]] = {
    // TODO when parent is set, low and high need special treatment
    var options1 = JsonObject()

    val isReverse = jgetBoolean(options, "reverse")

    val rincludeLow = jgetBoolean(options, "includelow")
    val rincludeHigh = jgetBoolean(options, "includeHigh")
    val rlow = jget(options, "low") match {
      case null => ""
      case j: Json => keyEncode(j)
    }
    val rhigh = jget(options, "high") match {
      case null => "~"
      case j: Json => keyEncode(j)
    }

    val (plow, phigh, pincludeLow, pincludeHigh) = jget(options, "prefix") match {
      case null => ("", "~", false, false)
      case j: Json => {
        val k = keyEncode(j)
        val includePrefix = jgetBoolean(options, "includeprefix")
        (k, k + "\uFFFF", includePrefix, false)
      }
    }

    val (tlow, thigh, tincludeLow, tincludeHigh, parent) = jget(options, "parent") match {
      case null => ("", "~", false, false, null)
      case j: Json => {
        val k = keyEncode(j)
        // TODO must be and array
        val includeParent = jgetBoolean(options, "includeParent")
        (k, k + "\uFFFF", includeParent, false, j)
      }
    }

    val (low1, includeLow1) = {
      if (rlow == plow) {
        (rlow, rincludeLow && pincludeLow)
      } else if (rlow < plow) {
        (plow, pincludeLow)
      } else {
        (rlow, rincludeLow)
      }
    }

    val (high1, includeHigh1) = {
      if (rhigh == phigh) {
        (rhigh, rincludeHigh && pincludeHigh)
      } else if (rhigh > phigh) {
        (phigh, pincludeHigh)
      } else {
        (rhigh, rincludeHigh)
      }
    }

    val (low, includeLow) = {
      if (low1 == tlow) {
        (low1, includeLow1 && tincludeLow)
      } else if (low1 < tlow) {
        (tlow, tincludeLow)
      } else {
        (low1, includeLow1)
      }
    }

    val (high, includeHigh) = {
      if (high1 == thigh) {
        (high1, includeHigh1 && tincludeHigh)
      } else if (thigh > high1) {
        (high1, includeHigh1)
      } else {
        (thigh, tincludeHigh)
      }
    }

    jgetString(options, "get") match {
      case "" =>
      case s => options1 = options1 + ("get" -> s)
    }

    val ni = if (isReverse) {
      new BackwardNextItem(null, high,
        tableName, includeHigh, Compact(options1), low, includeLow, parent,
        send, system)
    } else {
      new ForwardNextItem(null, low,
        tableName, includeLow, Compact(options1), high, includeHigh, parent,
        send, system)
    }
    ni.next()
  }

}