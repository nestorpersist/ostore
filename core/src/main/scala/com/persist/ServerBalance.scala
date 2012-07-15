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
import akka.actor.ActorSystem
import akka.actor.ActorRef

private[persist] trait ServerBalanceComponent { this: ServerTableAssembly =>
  val bal: ServerBalance
  class ServerBalance(system:ActorSystem) {
    var canSend = false
    var canReport = false
    var threshold:Long = 1 // should be at least 1
    
    var cntToNext: Long = 0
    var cntFromPrev: Long = 0
    
    // next node
    var nextNodeName = ""
    private var nextNode:ActorRef = null
    var nextCount: Long = Long.MaxValue
    var nextLow: String = info.high

    // prev node
    private var prevNodeName =  ""
    private var prevNode:ActorRef = null
    
    setPrevNextName()
    
    var singleNode = info.nodeName == nextNodeName
    
    private def setPrevNextName():Boolean = {
       val nextNodeName = info.config.rings(info.ringName).nextNodeName(info.nodeName)
       val prevNodeName =  info.config.rings(info.ringName).prevNodeName(info.nodeName)
       val changed = nextNodeName != this.nextNodeName || prevNodeName != this.prevNodeName
      this.prevNodeName = prevNodeName
      this.nextNodeName = nextNodeName
      changed
    }

    def setPrevNext() {
      nextNode = info.config.getRef(system,info.ringName,nextNodeName,info.tableName)
      prevNode = info.config.getRef(system,info.ringName, prevNodeName, info.tableName)    
    }
    
    def resetPrevNext() {
      if (setPrevNextName() || nextNode == null) {
        setPrevNext()
      }
      singleNode = info.nodeName == prevNodeName
    }

    def inNext(key:String) = {
      if (singleNode) {
        false
      } else {
        nextLow.startsWith(key)
      }
    }
    
    def sendPrefix(prefix:String,key:String,meta:String,v:String) {
      val request = JsonObject("p"->prefix,"k"->key,"m"->meta,"v"->v)
      nextNode ! ("fromPrev", Compact(request))
    }
    
    def isBusy:Boolean = {
      val count = info.storeTable.size()
      count > 1 && info.high != nextLow
    }

    def sendToNext {
      val count = info.storeTable.size()
      if (count == 0) return // nothing to send
      if (count == 1) return // to ensure there is always at least 1 negative range in ring
      if (info.high != nextLow) return // send already in progress
      val incr = if (info.low > info.high) 1 else 0   
      if (count  < nextCount + incr + threshold) return // next is not enough smaller
      val key = info.storeTable.prev(nextLow, false) match {
        case Some(key) => { key }
        case None => {
          info.storeTable.last match {
            case Some(key) => { key }
            case None => {
              return // should not occur
            }
          }
        }
      }
      val meta = info.storeTable.getMeta(key) match {
        case Some(value: String) => value
        case None => {
          return // should not occur
        }
      }
      val v = info.storeTable.get(key) match {
        case Some(value: String) => value
        case None => {
          return // should not occur
        }
      }
      info.high = key
      info.storeTable.put("!high", info.high)
      nextCount += 1
      cntToNext += 1
      val uid = info.uidGen.get
      val request = JsonObject("t"->uid,"k"->key,"m"->meta,"v"->v)
      // TODO send prefixes if needed
      // if has prefixes
      // if prefixes of item sent not already on next node
      //info.nextNode ! ("fromPrev", uid, key, meta, v)
      nextNode ! ("fromPrev", Compact(request))
    }

    //def fromPrev(uid:Long, key: String, meta: String, value: String) {
    def fromPrev(r:String) {
      // TODO if has prefixes
      // if current prefix not present add the prefix
      // else if current prefix is older -- run map prefix on items except sent
      // else if prefix is newer - run map prefix on new item 
      // ack the prefixes somewhere???
      val request = Json(r)
      val t = jgetLong(request,"t")
      val prefix = jgetString(request,"p")
      val key = jgetString(request,"k")
      val meta = jgetString(request,"m")
      val value = jgetString(request,"v")
      if (prefix != "") {
        mr.map(key,prefix,meta,value)
        val response = JsonObject("p"->JsonObject("p"->prefix,"k"->key,"m"->meta))
        prevNode ! ("fromNext", Compact(response))
        return
      }
      
      if (t != 0) info.uidGen.set(t) // sync current clock to prev clock
      cntFromPrev += 1
      info.storeTable.putBoth(key, meta, value)
      info.low = key
      info.storeTable.put("!low", info.low)
      //info.prevCount = pcount
      if (mr.hasReduce) mr.reduceOut(key,info.absentMetaS,"null",meta,value)
    }

    private var prevReportedCount: Long = 0
    private var prevReportedLow = info.low

    def reportToPrev {
      val count = info.storeTable.size()
      if ((canSend && prevReportedCount != count) || prevReportedLow != info.low) {
        //info.prevNode ! ("fromNext", count, info.low)
        val response = JsonObject("n"->count,"k"->info.low)
        prevNode ! ("fromNext", Compact(response))
        prevReportedCount = count
        prevReportedLow = info.low
      }
    }

    //def fromNext(ncount: Long, nlow: String) {
    def fromNext(res:String) {
      val response = Json(res)
      val prefix = jgetString(response,"p","p")
      if (prefix != "") {
        val key = jgetString(response,"p","k")
        val meta = jgetString(response,"p","m")
        mr.ackPrefix(prefix,key,meta)
        return
      }
      val ncount = jgetLong(response,"n")
      val nlow = jgetString(response,"k")
      if (nextLow != nlow) {
        for (k <- info.range(nlow, nextLow, singleNode)) {
          if (mr.hasReduce) {
            val oldMeta = info.storeTable.getMeta(k) match {
              case Some(s:String) => s
              case None => ""
            }
            val oldValue = info.storeTable.get(k) match {
              case Some(s:String) => s
              case None => ""
            }
            mr.reduceOut(k,oldMeta,oldValue,info.absentMetaS,"null")
          }
          info.storeTable.remove(k)
        }
        nextLow = nlow
      }
      nextCount = ncount
    }
  }
}