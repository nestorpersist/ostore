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

import akka.actor.ActorRef
import JsonOps._
import JsonKeys._
import akka.dispatch.ExecutionContext
import akka.util.Timeout
import akka.util.duration._
import Exceptions._
import ExceptionOps._
import Codes.emptyResponse
import Stores._

private[persist] trait ServerTableAssembly extends ServerTableMapComponent with ServerTableReduceComponent with ServerTableSyncComponent
  with ServerTableBalanceComponent with ServerTableOpsComponent with ServerTableInfoComponent with ServerTableBackgroundComponent

private[persist] class ServerTable(databaseName: String, ringName: String, nodeName: String, tableName: String,
  store: Store, monitor: ActorRef, send: ActorRef, initialConfig: DatabaseConfig) extends CheckedActor {
  
  object all extends ServerTableAssembly {
    val info = check(new ServerTableInfo(databaseName, ringName, nodeName, tableName,
      initialConfig, send, store, monitor, log))
    val ops = check(new ServerTableOps(context.system))
    val map = check(new ServerTableMap)
    val reduce = check(new ServerTableReduce)
    val sync = check(new ServerTableSync)
    val bal = check(new ServerTableBalance(context.system))
    val back = check(new ServerTableBackground)
  }

  val info = all.info
  val ops = all.ops
  val map = all.map
  val reduce = all.reduce
  val bal = all.bal
  val sync = all.sync
  val back = all.back

  private val system = context.system
  lazy implicit private val ec = ExecutionContext.defaultExecutionContext(system)
  implicit private val timeout = Timeout(5 seconds)

  /*
  private def checkKey(k: String, less: Boolean): Boolean = {
    if (bal.singleNode) return true
    if (info.low > info.high) {
      if (less) {
        if (info.low < k || k <= info.high) return true
      } else {
        if (info.low <= k || k < info.high) return true
      }
    } else {
      if (less) {
        if (info.low < k && k <= info.high) return true
      } else {
        if (info.low <= k && k < info.high) return true
      }
    }
    false
  }
  */

  def makeArray(store: StoreTable, low: String, high: String, options: JsonObject): Json = {
    var v = JsonArray()
    for (s <- info.range(store, low, high, bal.singleNode, options)) {
      var v1 = JsonArray()
      v1 = keyDecode(s) +: v1
      val meta = store.getMeta(s) match {
        case Some(m: String) => Json(m)
        case None => JsonObject()
      }
      v1 = meta +: v1
      val v2 = store.get(s) match {
        case Some(v1: String) => Json(v1)
        case None => null
      }
      v1 = v2 +: v1
      v = v1.reverse +: v
    }
    return v.reverse
  }

  def report(): (String, Any) = {
    var r = JsonObject("low" -> keyDecode(info.low),
      "high" -> keyDecode(info.high), "vals" -> makeArray(info.storeTable, info.low, info.high, emptyJsonObject))
    if (info.high != bal.nextLow) {
      r = r + ("nextLow" -> keyDecode(bal.nextLow))
      r = r + ("transit:" -> makeArray(info.storeTable, info.high, bal.nextLow, emptyJsonObject))
    }
    if (map.prefixes.size > 0) {
      var prefixes = JsonObject()
      for ((name, pinfo) <- map.prefixes) {
        val plow = keyEncode(info.getPrefix(keyDecode(info.low), pinfo.size))
        val phigh = keyEncode(info.getPrefix(keyDecode(info.high), pinfo.size))
        prefixes += (name -> makeArray(pinfo.storeTable, plow, phigh, JsonObject("prefixtab" -> name, "includehigh" -> true)))
      }
      r += ("prefixes" -> prefixes)
    }
    (Codes.Ok, Compact(r))
  }

  def doBasicCommand(kind: String, key: String, value: Any): (String, Any) = {
    (kind, value) match {
      case ("map", (prefix: String, meta: String, v: String)) => {
        val (code, result) = map.map(key, prefix, meta, v)
        (code, result)
      }
      case ("reduce", (node: String, item: String, t: Long)) => {
        val (code, result) = reduce.reduce(key, node, item, t)
        (code, result)
      }
      case ("sync", (oldcv: String, oldv: String, cv: String, v: String)) => {
        val (code, result) = sync.sync(key, oldcv, v, cv, v)
        (code, result)
      }
      case x => {
        if (ops.acceptUserMessages) {
          ops.cntMsg += 1
          (kind, value) match {
            case ("get", v: String) => ops.get(key, v)
            case ("put", (v: String, os: String)) => {
              if (ops.canWrite) {
                ops.put(key, v, os)
              } else {
                (Codes.ReadOnly, Compact(JsonObject("table" -> info.tableName)))
              }
            }
            case ("delete", v: String) => {
              if (ops.canWrite) {
                ops.delete(key, v)
              } else {
                (Codes.ReadOnly, info.tableName)
              }
            }
            case ("resync", v: String) => ops.resync(key, v)
            case ("next", v: String) => ops.next(kind, key, v)
            case ("next+", v: String) => ops.next(kind, key, v)
            case ("prev", v: String) => ops.prev(kind, key, v)
            case ("prev-", v: String) => ops.prev(kind, key, v)
            case (badKind: String, v: String) => {
              log.error("table unrecognized command:" + badKind)
              (Codes.InternalError, Compact(JsonObject("msg" -> "unrecognized kind", "kind" -> badKind)))
            }
          }
        } else {
          (Codes.NotAvailable, emptyResponse)
        }
      }
    }
  }

  def doCommand(cmd: Any) {
    cmd match {
      case ("init") => {
        sender ! Codes.Ok
      }
      case ("start", balance: Boolean, user: Boolean) => {
        if (balance) {
          if (!bal.singleNode) {
            bal.canSend = true
            bal.canReport = true
          }
          bal.setPrevNext()
        }
        if (user) ops.acceptUserMessages = true
        sender ! Codes.Ok
      }
      case ("stop", user: Boolean, balance: Boolean, forceBalance: Boolean) => {
        if (user) ops.acceptUserMessages = false
        if (balance) bal.canSend = false
        if (forceBalance) {
          bal.canSend = true
          bal.forceEmpty = true
        }
        sender ! Codes.Ok
      }
      case ("stop2") => {
        map.close
        reduce.close
        info.storeTable.putControl("!clean", "true")
        info.storeTable.close()
        sender ! Codes.Ok
      }
      case ("delete2") => {
        map.delete
        reduce.delete
        info.storeTable.store.deleteTable(info.storeTable.tableName)
        sender ! Codes.Ok

      }
      case ("busyBalance") => {
        val code = if (bal.isBusy) {
          Codes.Busy
        } else {
          Codes.Ok
        }
        sender ! code
      }
      case ("getLowHigh") => {
        val result = JsonObject("low" -> info.low, "high" -> info.high)
        sender ! (Codes.Ok, result)
      }
      case ("setLowHigh", low: String, high: String) => {
        info.low = low
        info.high = high
        bal.nextLow = high
        sender ! Codes.Ok
      }
      case ("addRing", ringName1: String, config: DatabaseConfig) => {
        info.config = config
        if (ringName == ringName1) bal.setPrevNext()
        sender ! Codes.Ok
      }
      case ("copyRing", ringName1: String) => {
        back.ringCopyTask(ringName1)
        sender ! Codes.Ok
      }
      case ("ringReady", ringName: String) => {
        val code = if (back.ringCopyActive(ringName)) Codes.Busy else Codes.Ok
        sender ! code
      }
      case ("fromPrev", request: String) => {
        bal.fromPrev(request)
      }
      case ("fromNext", response: String) => {
        bal.fromNext(response)
      }
      case ("setConfig", config: DatabaseConfig) => {
        info.config = config
        bal.setPrevNextName()
        sender ! Codes.Ok
      }
      case (kind: String, uid: Long, key: String, value: Any) => {
        //val sender1 = sender  // must preserve sender for futures
        back.qcnt += 1
        try {
          val (code, result) = kind match {
            case "report" => report()
            case x => {
              val less = kind == "prev-"
              if (info.checkKey(key, less, info.low, info.high)) {
                doBasicCommand(kind, key, value)
              } else {
                val server = info.config.rings(ringName).nodes(bal.nextNodeName).server
                val host = server.host
                val port = server.port
                val response = JsonObject("low" -> info.low, "high" -> info.high, "next" -> bal.nextNodeName)
                //  "host" -> host, "port" -> port)
                (Codes.Handoff, Compact(response))
              }
            }
          }
          sender ! (code, uid, result)
          /*
          f map { pair =>
            val (code,result) = pair
            //println("RESPOND:"+sender+":"+code+":"+result+":"+sender1)
            sender1 ! (code, uid, result)
          } recover { case ex => {
            sender1 ! (Codes.InternalError, uid, ex.getMessage())
            println("Table internal error: " + kind + ":" + ex.toString())
            ex.printStackTrace()
            }
          }
          */
        } catch {
          case ex: Exception => {
            val (code, v1) = exceptionToCode(ex)
            sender ! (code, uid, v1)
            log.error(ex, "ServerTable Exception")
          }
        }
      }
      case ("token", t: Long) => {
        back.token(t)
      }
      case other => {
        log.error("Bad table command form:" + cmd)
      }
    }
  }

  def rec = {
    case cmd => {
      doCommand(cmd)
      back.act(self)
      monitor ! ("report", tableName, bal.cntToNext, bal.cntFromPrev, sync.cntSync, ops.cntMsg, info.storeTable.size())
    }
  }
}