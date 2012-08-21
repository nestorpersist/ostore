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
import scala.collection.immutable.HashMap

object Exceptions {

  private val httpCode = HashMap[String, (Int, String)](
    Codes.JsonParse -> (400, "BAD_REQUEST"),
    Codes.JsonUnparse -> (400, "BAD_REQUEST"),
    Codes.BadRequest -> (400, "BAD_REQUEST"),
    Codes.NoDatabase -> (404, "NOT_FOUND"),
    Codes.NoRing -> (404, "NOT_FOUND"),
    Codes.NoNode -> (404, "NOT_FOUND"),
    Codes.NoTable -> (404, "NOT_FOUND"),
    Codes.Conflict -> (409, "CONFLICT"))

  class SystemException(val kind: String, val info: Json) extends Exception {
    override def toString(): String = kind + ":" + Compact(info)
  }

  class JsonParseException(msg: String, input: String, line: Int, char: Int)
    extends SystemException(Codes.JsonParse, JsonObject("msg" -> msg, "line" -> line, "char" -> char, "input" -> input)) {
    override def toString() = {
      "[" + jgetInt(info, "line") + "," + jgetInt(info, "char") + "] " + jgetString(info, "msg") + " (" + jgetString(info, "input") + ")"
    }
  }

  private[persist] def getExceptionJson(ex: Exception): Json = {
    val name = ex.getClass().getCanonicalName()
    val msg = ex.getMessage()
    JsonObject("class" -> name, "msg" -> msg)
  }

  def InternalException(msg: String) = new SystemException(Codes.InternalError, JsonObject("msg" -> msg))

  def RequestException(msg: String) = new SystemException(Codes.BadRequest, JsonObject("msg" -> msg))

  private[persist] def exceptionToCode(ex: Exception): (String, String) = {
    ex match {
      case ex1: SystemException => {
        (ex1.kind, Compact(ex1.info))
      }
      case ex2 => {
        (Codes.InternalError, Compact(getExceptionJson(ex2)))
      }
    }
  }

  private[persist] def exceptionToHttp(ex: Exception): (Int, String, Json) = {
    ex match {
      case ex1: SystemException => {
        val body = JsonObject("kind" -> ex1.kind, "info" -> ex1.info)
        httpCode.get(ex1.kind) match {
          case Some((httpCode, short)) => {
            (httpCode, short, body)
          }
          case None => {
            (500, "INTERNAL_ERROR", body)

          }
        }
      }
      case ex2 => {
        (500, "INTERNAL_ERROR", getExceptionJson(ex2))
      }
    }
  }

  private[persist] def checkCode(code: String, info: String) {
    if (code != Codes.Ok) {
      throw new SystemException(code, Json(info))
    }
  }

  private[persist] def checkName(name: String) {
    var bad = false
    if (name.length() == 0) {
      bad = true
    } else {
      if (!name(0).isLetter) bad = true
    }
    for (ch <- name) {
      if (!ch.isLetter && !ch.isDigit) bad = true
    }
    if (bad) {
      throw new SystemException(Codes.BadRequest, JsonObject("msg" -> "bad name", "name" -> name))
    }
  }

  private def checkNamedConfig(config: Json) {
    config match {
      case t: JsonObject => {
        t.get("name") match {
          case Some(s: String) => checkName(s)
          case Some(x) => throw new SystemException(Codes.BadRequest, JsonObject("msg" -> "bad name", "name" -> x))
          case None => throw new SystemException(Codes.BadRequest, JsonObject("msg" -> "missing name", "config" -> t))
        }
      }
      case x => throw new SystemException(Codes.BadRequest, JsonObject("msg" -> "bad config", "config" -> x))
    }
  }

  private[persist] def checkConfig(config: Json) {
    for (table <- jgetArray(config, "tables")) {
      checkNamedConfig(table)
    }
    for (ring <- jgetArray(config, "rings")) {
      checkNamedConfig(ring)
      for (node <- jgetArray(ring, "nodes")) {
        checkNamedConfig(node)
      }
    }
  }
}

