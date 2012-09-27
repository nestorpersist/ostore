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

/**
 * This object defines the exception/error codes for OStore.
 * These codes can appear in SystemException and in REST results.
 */
object Codes {
  
  private[persist] final val emptyResponse = Compact(JsonObject())
  
  // Success
  private[persist] final val Ok = "Ok"
  private[persist] final val Busy = "Busy" // activity still ongoing
    
  /**
   * Database is currently locked by different user
   */
  final val Locked = "Locked" 

  // Handoffs
  private[persist] final val Handoff = "Handoff"
  private[persist] final val Next = "Next"
  private[persist] final val Prev = "Prev"
  private[persist] final val PrevM = "Prev-"
  private[persist] final val Done = "Done"

  // Failures
    
  /**
   * Named database does not exist.
   */
  final val NoDatabase = "NoDatabase"
    
  /**
   * Named ring does not exist.
   */
  final val NoRing = "NoRing"

   /**
   * Named node does not exist.
   */
  final val NoNode = "NoNode"
    
  /**
   * Named table does not exist.
   */
  final val NoTable = "NoTable"
    
  /** 
   * Item with specified key does not exist.
   */
  final val NoItem = "NoItem"
    
  /**
   * Named database already exists.
   */
  final val ExistDatabase = "ExistDatabase"
    
  /**
   * Named table already exists.
   */
  final val ExistTable = "ExistTable"
    
  /** 
   * Named ring already exists.
   */
  final val ExistRing = "ExistRing"
    
  /**
   * Named node already exists.
   */
  final val ExistNode = "ExistNode"

  /**
   * Optimistic put or delete failed.
   */
  final val Conflict = "Conflict" 

  /**
   * Error when parsing Json.
   */
  final val JsonParse = "JsonParse"
   
   /**
    * Error when unparsing (converting tree to string) Json.
    */
  final val JsonUnparse = "JsonUnparse"

  /** 
   * Feature not yet implemented.
   */
  final val NYI = "NYI"
    
  /**
   * Communication timed out.
   */
  final val Timeout = "Timeout"

  /**
   * Attempt to modify a read-only value.
   */
  final val ReadOnly = "ReadOnly"

  /**
   * Internal system error. These normally indicate
   * a bug and should be reported.
   */
  final val InternalError = "InternalError"
  
  /**
   * A user request was ill-formed or missing required information.
   */
  final val BadRequest = "BadRequest"

  /**
   * Service is not currently available.
   */
  final val NotAvailable = "NotAvailable"
}
