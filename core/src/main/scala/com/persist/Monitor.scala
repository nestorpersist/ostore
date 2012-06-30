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
import akka.actor.ActorRef

private[persist] class Monitor(nodeName: String) extends CheckedActor {

  var tables = JsonObject()

  def rec = {
    case ("report", tableName: String, toNext: Long, fromPrev: Long, sync: Long, msg: Long, size: Long) => {
      var info = jgetObject(tables,tableName)
      info = info ++ List("msg" -> msg, "size" -> size, "toNext" -> toNext, "fromPrev" -> fromPrev,
      "sync" -> sync)
      tables = tables + (tableName -> info)
    }
    case ("reportget", tableName:String, getCnt:Long,getTime:Long) => {
      var info = jgetObject(tables,tableName)
      info = info ++ List("get" -> getCnt, "getTime" -> getTime)
      tables = tables + (tableName -> info)
    }
    case ("reportput", tableName:String, putCnt:Long,putTime:Long) => {
      var info = jgetObject(tables,tableName)
      info = info ++ List("put" -> putCnt, "putTime" -> putTime)
      tables = tables + (tableName -> info)
    }
    case ("get", tableName: String) => {
      val info = jgetObject(tables, tableName)
      sender ! (Codes.Ok, Compact(info))
    }
    case ("stop") => {
      sender ! Codes.Ok
    }
    case x => println("monitorfail:" + x)
  }

}