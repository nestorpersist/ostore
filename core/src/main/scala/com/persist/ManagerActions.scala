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
import java.util.UUID
import akka.dispatch.Future
import akka.util.Timeout
import akka.pattern._
import akka.util.duration._
import akka.dispatch.Await
import Exceptions._
import akka.actor.ActorRef
import java.util.concurrent.TimeoutException

private[persist] class ManagerActions(serverName: String, server: ActorRef, messaging: ManagerMessaging) {

  private implicit val timeout = Timeout(5 seconds)

  case class Act(databaseName: String) {

    val guid = UUID.randomUUID().toString()
    private var servers1 = List[String]()
    def servers = servers1
    private var locked = List[String]()

    private def checkInitialResponse(expectedState: String, check: Json, response: Json) {
      val tableName = jgetString(check, "table")
      val ringName = jgetString(check, "ring")
      val nodeName = jgetString(check, "node")
      val state = jgetString(response, "s")
      if (expectedState != "" && expectedState != state) {
        val f = server ? ("unlock", guid, databaseName, emptyJsonObject)
        val (code, s: String) = Await.result(f, 5 seconds)
        throw InternalException("Bad state, expected " + expectedState + " actual " + state)
      }
      val actualDatabaseAbsent = jgetBoolean(response, "databaseAbsent")
      var ex: Exception = null
      val requestDatabaseAbsent = jgetBoolean(check, "databaseAbsent")
      if (requestDatabaseAbsent) {
        if (!actualDatabaseAbsent) {
          ex = new SystemException(Codes.ExistDatabase, JsonObject("database" -> databaseName))
        }
      } else {
        if (actualDatabaseAbsent) {
          ex = new SystemException(Codes.NoDatabase, JsonObject("database" -> databaseName))
        }
      }
      if (tableName != "") {
        val requestTableAbsent = jgetBoolean(check, "tableAbsent")
        val actualTableAbsent = jgetBoolean(response, "tableAbsent")
        if (requestTableAbsent) {
          if (!actualTableAbsent) {
            ex = new SystemException(Codes.ExistTable, JsonObject("table" -> tableName))
          }
        } else {
          if (actualTableAbsent) {
            ex = new SystemException(Codes.NoTable, JsonObject("table" -> tableName))
          }
        }
      }
      if (nodeName != "") {
        val requestNodeAbsent = jgetBoolean(check, "nodeAbsent")
        val actualNodeAbsent = jgetBoolean(response, "nodeAbsent") || jgetBoolean(response, "ringAbsent")
        if (requestNodeAbsent) {
          if (!actualNodeAbsent) {
            ex = new SystemException(Codes.ExistNode, JsonObject("ring" -> ringName, "node" -> nodeName))
          }
        } else {
          if (actualNodeAbsent) {
            ex = new SystemException(Codes.NoNode, JsonObject("ring" -> ringName, "node" -> nodeName))
          }
        }
      }
      if (nodeName == "" && ringName != "") {
        val requestRingAbsent = jgetBoolean(check, "ringAbsent")
        val actualRingAbsent = jgetBoolean(response, "ringAbsent")
        if (requestRingAbsent) {
          if (!actualRingAbsent) {
            ex = new SystemException(Codes.ExistRing, JsonObject("ring" -> ringName))
          }
        } else {
          if (actualRingAbsent) {
            ex = new SystemException(Codes.NoRing, JsonObject("ring" -> ringName))
          }
        }
      }
      if (ex != null) {
        val f = server ? ("unlock", guid, databaseName, emptyJsonObject)
        val (code, s: String) = Await.result(f, 5 seconds)
        throw ex
      }
    }

    private def doAll(cmd: String, request: JsonObject, servers1: List[String] = servers): Boolean = {
      val rs = Compact(request)
      var ok = true
      val futures: List[Future[Any]] = servers1.map(serverName => {
        val server = messaging.serverRef(serverName)
        server ? (cmd, guid, databaseName, rs)
      })
      var finalex: Throwable = null
      for ((f, serverName) <- futures.zip(servers1)) {
        def info = JsonObject("database" -> databaseName, "cmd" -> cmd, "server" -> serverName)
        try {
          val (code: String, response) = Await.result(f, 5 seconds)
          if (code == Codes.Ok) {
            if (cmd == "lock") locked = serverName :: locked
          } else if (code == Codes.Busy) {
            ok = false
          } else {
            if (finalex != null) finalex = new SystemException(code, info)
          }
        } catch {
          case ex: TimeoutException => if (finalex != null) finalex = new SystemException(Codes.Timeout, info)
          case ex => if (finalex != null) finalex = new SystemException(Codes.InternalError, info + ("msg" -> ex.toString))
        }
      }
      if (finalex != null) throw finalex
      ok
    }

    private def initialLock(expectedState: String, check: JsonObject) = {
      var request = JsonObject("getinfo" -> true)
      val tableName = jgetString(check, "table")
      if (tableName != "") request += ("table" -> tableName)
      val ringName = jgetString(check, "ring")
      if (ringName != "") request += ("ring" -> ringName)
      val nodeName = jgetString(check, "node")
      if (nodeName != "") request += ("node" -> nodeName)
      def info = JsonObject("database" -> databaseName, "cmd" -> "lock", "server" -> serverName)

      val f = server ? ("lock", guid, databaseName, Compact(request))
      val (code: String, s: String) = try {
        Await.result(f, 5 seconds)
      } catch {
        case ex: TimeoutException => throw new SystemException(Codes.Timeout, info)
        case ex => throw new SystemException(Codes.InternalError, info + ("msg" -> ex.toString))
      }
      if (code != Codes.Ok) {
        if (code == Codes.Lock) throw new SystemException(Codes.Lock, info)
        throw new SystemException(code, info)
      }
      locked = serverName :: locked

      val response = Json(s)
      checkInitialResponse(expectedState, check - "servers", response)
      val actualDatabaseAbsent = jgetBoolean(response, "databaseAbsent")
      val serversArray = if (actualDatabaseAbsent) {
        jgetArray(check, "servers")
      } else {
        jgetArray(response, "servers")
      }
      servers1 = serversArray.map(jgetString(_)).toList

      doAll("lock", emptyJsonObject, servers.filterNot(_ == serverName))
    }

    def act(expectedState: String = "", check: JsonObject = emptyJsonObject)(body: => Unit) {
      var ex:Throwable = null
      try {
        initialLock(expectedState, check)
      } catch {
        case ex1: Throwable => ex = ex1
      }
      try {
        // do body only if all servers locked
        // don't unlock if body fails
        if (ex == null) body
        doAll("unlock", emptyJsonObject, locked.reverse)
      } catch {
        case ex1: Throwable => if (ex == null) ex = ex1
      }
      if (ex != null) throw ex // first failure
    }

    def lock(servers1: List[String])(body: => Unit) = {
      doAll("lock", emptyJsonObject, servers1)
    }

    def pass(cmd: String, request: JsonObject = emptyJsonObject, servers: List[String] = servers) {
      doAll(cmd, request, servers)
    }

    def wait(cmd: String, request: JsonObject = emptyJsonObject) {
      for (i <- 1 until 60) { // try for 2 minutes
        try {
          doAll(cmd, request)
          return
        } catch {
          case ex: SystemException => if (ex.kind != Codes.Busy) throw ex
        }
        Thread.sleep(2000) // 2 seconds
      }
      throw new SystemException(Codes.Timeout, "Wait " + cmd + " timeout")
    }
  }
}
