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
import akka.actor.ActorLogging
import Exceptions._
import JsonOps._

private[persist] abstract class CheckedActor extends Actor with ActorLogging {
  def rec: PartialFunction[Any, Unit]
  def receive: PartialFunction[Any, Unit] = {
    case msg => {
      try {
        val body1: PartialFunction[Any, Unit] = rec.orElse {
          case x: Any => {
            val info = JsonObject(
              "msg" -> msg.toString(),
              "from" -> sender.toString(),
              "to" -> self.toString())
            val s = "Unmatched message " + Compact(info)
            log.error(s)
          }
        }
        body1(msg)
      } catch {
        case ex: Exception => {
          val info = JsonObject(
            "msg" -> msg.toString(),
            "from" -> sender.toString(),
            "to" -> self.toString(),
            "ex" -> ex.toString())
          val s = "Unhandled exception " + Compact(info)
          log.error(ex, s)
        }
      }
    }
  }
}
