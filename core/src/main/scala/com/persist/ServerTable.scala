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
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.Promise

private[persist] trait ServerTableAssembly extends ServerMapReduceComponent with ServerSyncComponent
  with ServerBalanceComponent with ServerOpsComponent with ServerTableInfoComponent

private[persist] class ServerTable(databaseName: String, ringName: String, nodeName: String, tableName: String,
  store: Store, monitor: ActorRef, send: ActorRef, config: DatabaseConfig) extends CheckedActor {

  object all extends ServerTableAssembly {
    val info = new ServerTableInfo(databaseName, ringName, nodeName, tableName,
      config, send, store, monitor)
    val ops = new ServerOps(context.system)
    val mr = new ServerMapReduce
    val sync = new ServerSync
    val bal = new ServerBalance(context.system)
  }

  val info = all.info
  val ops = all.ops
  val mr = all.mr
  val bal = all.bal
  val sync = all.sync

  private val system = context.system
  lazy implicit private val ec = ExecutionContext.defaultExecutionContext(system)
  implicit private val timeout = Timeout(5 seconds)

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

  def makeArray(low: String, high: String): Json = {
    var v = JsonArray()
    for (s <- info.range(low, high, bal.singleNode)) {
      var v1 = JsonArray()
      v1 = keyDecode(s) +: v1
      val meta = info.storeTable.getMeta(s) match {
        case Some(m: String) => Json(m)
        case None => JsonObject()
      }
      v1 = meta +: v1
      val v2 = info.storeTable.get(s) match {
        case Some(v1: String) => Json(v1)
        case None => null
      }
      v1 = v2 +: v1
      v = v1.reverse +: v
    }
    return v.reverse
  }

  def report(): (String, Any) = {
    var r = JsonObject("low" -> keyDecode(info.low), "high" -> keyDecode(info.high), "vals" -> makeArray(info.low, info.high))
    if (info.high != bal.nextLow) {
      r = r + ("nextLow" -> keyDecode(bal.nextLow))
      r = r + ("transit:" -> makeArray(info.high, bal.nextLow))
    }
    (Codes.Ok, Compact(r))
  }

  def doBasicCommand(kind: String, key: String, value: Any): (String, Any) = {
    (kind, value) match {
      case ("map", (prefix: String, meta: String, v: String)) => {
        val (code, result) = mr.map(key, prefix, meta, v)
        (code, result)
      }
      case ("reduce", (node: String, item: String, t: Long)) => {
        val (code, result) = mr.reduce(key, node, item, t)
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
                (Codes.ReadOnly, info.tableName)
              }
            }
            case ("delete", v: String) => {
              if (ops.canWrite) {
                ops.delete(key, v)
              } else {
                (Codes.ReadOnly, info.tableName)
              }
            }
            case ("next", v: String) => ops.next(kind, key, v)
            case ("next+", v: String) => ops.next(kind, key, v)
            case ("prev", v: String) => ops.prev(kind, key, v)
            case ("prev-", v: String) => ops.prev(kind, key, v)
            case (badKind: String, v: String) => {
              println("table unrecognized command:" + badKind)
              (Codes.BadRequest, badKind)
            }
          }
        } else {
          (Codes.NotAvailable, "")
        }
      }
    }
  }

  def doCommand(cmd: Any) {
    cmd match {
      case ("start1") => {
        sender ! Codes.Ok
      }
      case ("start2") => {
        if (!bal.singleNode) {
          bal.setPrevNext()
          bal.canSend = true
          bal.canReport = true
        }
        ops.acceptUserMessages = true
        sender ! Codes.Ok
      }
      case ("stop1") => {
        ops.acceptUserMessages = false
        bal.canSend = false
        // TODO wait for no in transit
        if (info.high != bal.nextLow) {
          println("Shutdown error /" +
            databaseName + "/" + ringName + "/" + nodeName + "/" + tableName + " (" + info.high + "," + bal.nextLow + ")")
        }
        // TODO wait for no pending in send
        sender ! Codes.Ok
      }
      case ("stop2") => {
        mr.close
        info.storeTable.put("!clean", "true")
        // TODO delete store table
        info.storeTable.close()
        sender ! Codes.Ok
      }
      case ("delete2") => {
        mr.delete
        info.storeTable.delete()
        sender ! Codes.Ok
        
      }
      case ("stopBalance", forceEmpty:Boolean) => {
        bal.canSend = forceEmpty
        bal.forceEmpty = forceEmpty
        sender ! Codes.Ok
      }
      case ("startBalance") => {
        if (! bal.singleNode) {
          bal.canSend = true
          bal.canReport = true
        }
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
        val result = JsonObject("low"->info.low, "high"->info.high)
        sender ! (Codes.Ok, result)
      }
      case ("setLowHigh", low:String, high:String) => {
        info.low = low
        info.high = high
        bal.nextLow = high
        sender ! Codes.Ok
      }
      //case ("fromPrev", uid:Long, key: String, meta: String, value: String) => {
      //bal.fromPrev(uid, key, meta, value)
      //}
      case ("fromPrev", request: String) => {
        bal.fromPrev(request)
      }
      //case ("fromNext", count: Long, low: String) => {
      //bal.fromNext(count, low)
      //}
      case ("fromNext", response: String) => {
        bal.fromNext(response)
      }
      case ("setConfig", config:DatabaseConfig) => {
        info.config = config
        bal.resetPrevNext()
        sender ! Codes.Ok
      }
      case (kind: String, uid: Long, key: String, value: Any) => {
        //val sender1 = sender  // must preserve sender for futures
        try {
          val (code, result) = kind match {
            case "report" => report()
            case x => {
              val less = kind == "prev-"
              if (checkKey(key, less)) {
                doBasicCommand(kind, key, value)
              } else {
                val server = config.rings(ringName).nodes(bal.nextNodeName).server
                val host = server.host
                val port = server.port
                val response = JsonObject("low" -> info.low, "high" -> info.high, "next" -> bal.nextNodeName,
                    "host"->host, "port" -> port)
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
            sender ! (Codes.InternalError, uid, ex.getMessage())
            println("Table internal error1: " + kind + ":" + ex.toString())
            ex.printStackTrace()
          }
        }
      }
      case other => {
        println("Bad table command form:" + cmd)
      }
    }
  }

  def rec = {
    case cmd => {
      doCommand(cmd)
      if (bal.canSend) bal.sendToNext
      if (bal.canReport) bal.reportToPrev
      monitor ! ("report", tableName, bal.cntToNext, bal.cntFromPrev, sync.cntSync, ops.cntMsg, info.storeTable.size())
    }
  }
}