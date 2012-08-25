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

object Codes {
  
  private[persist] final val emptyResponse = Compact(JsonObject())
  
  // Success
  private[persist] final val Ok = "Ok"
  private[persist] final val Busy = "Busy" // activity still ongoing
  private[persist] final val NotPresent = "NotPresent"
  private[persist] final val Locked = "Locked" // already locked by different guid

  // Does not exist
  final val NoDatabase = "NoDatabase"
  final val NoRing = "NoRing"
  final val NoNode = "NoNode"
  final val NoTable = "NoTable"
  final val NoItem = "NoItem"
    
  // Already exists
  final val ExistDatabase = "ExistDatabase"
  final val ExistRing = "ExistRing"
    
  // Not available
  final val AvailableDatabase = "AvailableDatabase"

  // Handoffs
  private[persist] final val Handoff = "Handoff"
  private[persist] final val Next = "Next"
  private[persist] final val Prev = "Prev"
  private[persist] final val PrevM = "Prev-"
  private[persist] final val Done = "Done"

  // Failures 
  final val NYI = "NYI"
  final val Timeout = "Timeout"
  final val Conflict = "Conflict" // opt put failed
  final val NotAvailable = "NotAvailable"
  final val ReadOnly = "ReadOnly"
  final val BadRequest = "BadRequest"
  final val InternalError = "InternalError"
  final val Exist = "Exist"
  final val JsonParse = "JsonParse"
  final val JsonUnparse = "JsonUnparse"
}
