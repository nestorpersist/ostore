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

object Exceptions {

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

  private[persist] def InternalError(msg: String) = new SystemException(Codes.InternalError, JsonObject("msg" -> msg))

  private[persist] def RequestError(msg: String) = new SystemException(Codes.BadRequest, JsonObject("msg" -> msg))

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
        ex1.kind match {
          case Codes.InternalError => {
            (500, "INTERNAL_SERVER_ERROR", body)
          }
          case _ => {
            (500, "UNKNOWN_SERVER_ERROR", body)
          }
        }
      }
      case ex2 => {
        (500, "INTERNAL_SERVER_ERROR", Compact(getExceptionJson(ex2)))
      }
    }
  }

  private[persist] def checkCode(code: String, info: String) {
    if (code != Codes.Ok) {
      throw new SystemException(code, Json(info))
    }
  }
}

