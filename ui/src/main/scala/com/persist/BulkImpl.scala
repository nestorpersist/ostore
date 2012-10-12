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
import java.io.InputStream
import java.io.InputStreamReader
import java.io.BufferedReader
import akka.actor.Actor
import akka.dispatch.Promise
import akka.dispatch.Future
import akka.dispatch.DefaultPromise
import akka.dispatch.Await
import akka.util.duration._
import akka.pattern._
import akka.actor.ActorSystem
import akka.actor.Props
import Exceptions._
import java.io.OutputStream
import java.io.PrintWriter
import akka.actor.PoisonPill

private[persist] object BulkImpl {

  private[persist] class Split(in: => InputStream) extends Iterable[(Json, Json)] {
    def iterator: Iterator[(Json, Json)] = {
      var hasLine = true
      var line = ""
      var lineNumber = 0
      new Iterator[(Json, Json)] {
        var ir = new InputStreamReader(in)
        var r = new BufferedReader(ir)
        private def getLine = {
          if (hasLine) {
            line = r.readLine()
            lineNumber += 1
            if (line == null) {
              hasLine = false
              r.close
            }
          } else {
            line = null
          }
        }
        getLine
        def hasNext = hasLine
        def next = {
          val parts = line.split("\t")
          if (parts.size >= 2) {
            // CHECK JSON
            val key = try {
              Json(parts(0))
            } catch {
              case ex: JsonParseException => throw new JsonParseException("key: " + ex.msg, parts(0), lineNumber, ex.char)
              case ex: Throwable => throw ex
            }
            val value = try {
              Json(parts(1))
            } catch {
              case ex: JsonParseException => throw new JsonParseException("value: " + ex.msg, parts(1), lineNumber, ex.char)
              case ex: Throwable => throw ex
            }
            getLine
            (key, value)
          } else {
            throw new JsonParseException("no tab", line, lineNumber, 1)
          }
        }
      }
    }
  }

  private class Run(split: Iterable[(Json, Json)], out: (Json, Json) => Future[Unit], f: Promise[Throwable], cnt: Int) extends Actor {
    implicit val executor = context.dispatcher
    val it = split.iterator
    for (i <- 1 to cnt) self ! "next"
    var done = 0
    var ex: Throwable = null
    def receive = {
      case "next" => {
        if (ex == null && it.hasNext) {
          try {
            val (key, value) = it.next
            val f = out(key, value)
            f map {
              _ => self ! "next"
            }
          } catch {
            case ex1: Throwable => {
              ex = ex1
              done += 1
              if (done == cnt) {
                f.success(ex)
              }
            }
          }
        } else {
          done += 1
          if (done == cnt) {
            f.success(ex)
          }
        }
      }
    }
  }

  private[persist] class Load(system: ActorSystem, in: Iterable[(Json, Json)], out: (Json, Json) => Future[Unit], cnt: Int = 0) {
    implicit val executor = system.dispatcher
    var f1 = new DefaultPromise[Throwable]
    val cnt1 = if (cnt > 0) cnt else 5
    val run = system.actorOf(Props(new Run(in, out, f1, cnt1)), name = "run")
    def waitDone = {
      val ex = Await.result(f1, 2 hours)
      val stopped = gracefulStop(run, 10 seconds)(system)
      Await.result(stopped, 10 seconds)
      if (ex != null) throw ex
    }
  }

  private class Download(in: Iterable[(Json, Json)], out: OutputStream) extends Actor {
    self ! "start"
    def receive = {
      case "start" => {
        download(in, out)
        self ! PoisonPill
      }
    }
  }

  private[persist] def download(system: ActorSystem, in: Iterable[(Json, Json)], out: OutputStream) {
    val download = system.actorOf(Props(new Download(in, out)), name = "download")
  }

  private[persist] def download(in: Iterable[(Json, Json)], out: OutputStream) {
    val w = new PrintWriter(out)
    for ((key, value) <- in) {
      w.print(Compact(key))
      w.print("\t")
      w.println(Compact(value))
      w.flush()
    }
    w.close()
  }

}