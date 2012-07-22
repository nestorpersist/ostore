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

private[persist] abstract class CheckedActor extends Actor with akka.actor.ActorLogging {
  def rec: PartialFunction[Any, Unit]
  def receive: PartialFunction[Any, Unit] = {
    case msg => {
      try {
        val body1: PartialFunction[Any, Unit] = rec.orElse {
          case x: Any => { 
            val s = "Unmatched message " + x.toString() + " : " + self.toString()
            println(s)
            log.error(s)
            }
        }
        body1(msg)
      } catch {
        case ex: Exception => {
          val s = "Unhandled exception in %s while processing %s".format(self.toString(), msg.toString())
          println(s)
          ex.printStackTrace()
          log.error(ex, s)
        }
      }
    }
  }
}
