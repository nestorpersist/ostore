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

import com.persist.JsonOps._
import scala.collection.immutable.HashMap

/**
 * This object defines the Exceptions used within Persist-Json.
 */
object Exceptions {

    /**
     * This is the basic system exception.
     * 
     * @param kind the kind of Exception.
     * @param info detailed information about the exception.
     */
  class SystemException(val kind: String, val info: Json) extends Exception {
    /**
     * Produces a standard text form to OStore exceptions.
     */
    override def toString(): String = kind + ":" + Compact(info)
  }

  /**
   * This subclass of SystemException is used for errors that occur parsing JSON. 
   * 
   * @param msg the error message.
   * @param input the string being parsed.
   * @param line the line where the error occurred.
   * @param char the character position of the error on the line where it occurred.
   */
  class JsonParseException(val msg: String, val input: String, val line: Int, val char: Int)
    extends SystemException("JsonParse", JsonObject("msg" -> msg, "line" -> line, "char" -> char, "input" -> input)) {
    /** 
     * There is a special version of toString for JSON parse errors.
     */
    def shortString() = {
      "[" + jgetInt(info, "line") + "," + jgetInt(info, "char") + "] " + jgetString(info, "msg")
      
    }
    override def toString() = {
      shortString + " (" + jgetString(info, "input") + ")"
    }
  }

}

