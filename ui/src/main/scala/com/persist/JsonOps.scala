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

import scala.collection.immutable.HashMap
import JsonParse._
import JsonUnparse._

/**
 *
 * Provides types and functions for working with Json types.
 *
 * Json is represented by immutable Scala types.
 * Instead of having separate Json types, type aliases are
 * defined for Json forms.
 *
 * Scala types used for Json are
 *
 *  - Json Object. Immutable Map[String,Json]
 *     - Note that keys are not ordered.
 *    -  When converting to a string with Compact or Pretty
 *       keys are sorted.
 *  - Json Array. Immutable Seq[Json]
 *  - Json String. String
 *  - Json Boolean. Boolean
 *  - Json Number. Int, Long, BigDecimal (with .), Double (with e)
 *  - Json Null. Null
 *
 * Any of the Json types can be at the top level
 * of a document (not just array and object).
 *
 * The Json parser supports some extensions that are useful for human edited
 * Json input (such as configurations).
 *
 *   - Comments. The characters // to end of line are discarded during parsing.
 *   - Scala-like raw strings. Start with ""{ and end with }"". Treated as normal strings after parsing.
 *   - No quotes on simple names. If an object component name starts with a letter and contains
 *     only letters and digits the " quotes are not required. After parsing names with and without
 *     quotes are not distinguished.
 *
 */
object JsonOps {

  /**
   *
   * A type alias for all Json forms.
   * Json values should be restriced by convention
   * to only those types used to represent Json.
   *
   */
  type Json = Any

  /**
   *
   * A type alias for of Json Objects.
   */
  type JsonObject = Map[String, Json]

  //type JsonPair = (String, Json)

  /**
   *
   * A constructor for JsonObject.
   *
   * @param Pairs a sequence of name value pairs.
   *
   * @return the constructed Json Object.
   */
  def JsonObject(pairs: (String, Json)*): JsonObject = new HashMap[String, Json]() ++ pairs

  /**
   *
   * An empty JsonObject.
   */
  val emptyJsonObject = JsonObject()

  /**
   *
   * A type alias for Json Arrays.
   */
  type JsonArray = Seq[Json]

  /**
   *
   * A constructor for JsonArray.
   *
   * @param elements a sequence of array element values.
   *
   * @return the constructed Json Array.
   *
   */
  def JsonArray(elements: Json*): JsonArray = List() ++ elements

  /**
   *
   * An empty JsonArray.
   *
   */
  val emptyJsonArray = JsonArray()

  /**
   *
   * A Json parser.
   *
   */
  def Json(s: String): Json = {
    parse(s)
  }

  /**
   *
   * A Json unparser. It produces the most compact single line string form.
   *
   */
  def Compact(j: Json): String = {
    compact(j)
  }

  /**
   *
   * A Json unparser. It produces a multiple line form
   * designed for human readability.
   *
   */
  def Pretty(j: Json): String = {
    //pretty(j)
    pretty(j)
  }

  /**
   *
   * Get a value within a nested Json value.
   *
   * @param a the input Json value.
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected value or null if not present.
   *
   */
  def jget(a: Json, ilist: Any*): Json = {
    if (ilist.size == 0) {
      a
    } else {
      val result = (a, ilist.head) match {
        case (a1: JsonArray, i1: Int) => {
          if (0 <= i1 && i1 < a1.size) { a1(i1) } else { null }
        }
        case (a1: JsonObject, i1: String) => {
          a1.get(i1) match {
            case Some(v) => v
            case None => null
          }
        }
        case _ => null
      }
      jget(result, ilist.tail: _*)
    }
  }

  /**
   *
   * Tests if a nested Json value is present.
   *
   * @param a the input Json value.
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return true if the selected field is present.
   *
   */
  def jhas(a: Json, ilist: Any*): Boolean = {
    if (ilist.size == 0) {
      true
    } else {
      (a, ilist.head) match {
        case (a1: JsonArray, i1: Int) => {
          if (0 <= i1 && i1 < a1.size) {
            if (ilist.size == 1) {
              true
            } else {
              jhas(a1(i1), ilist.tail: _*)
            }
          } else {
            false
          }
        }
        case (a1: JsonObject, i1: String) => {
          a1.get(i1) match {
            case Some(v) => {
              if (ilist.size == 1) {
                true
              } else {
                jhas(v, ilist.tail: _*)
              }
            }
            case None => false
          }
        }
        case _ => false
      }
    }
  }

  /**
   *
   * Replace a value within a nested Json value.
   *
   * @param a the input Json value.
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *  @param v the new value.
   *
   * @return a copy of the input with the value replaced.
   *
   */
  def jput(a: Json, ilist: Any*)(v: Json): Json = {
    if (ilist.size == 0) {
      a
    } else {
      (a, ilist.head) match {
        case (a1: JsonArray, i1: Int) => {
          if (0 <= i1 && i1 < a1.size) {
            val (a2, a3) = a1.splitAt(i1)
            val v1 = if (ilist.size == 1) {
              v
            } else {
              jput(a1(i1), ilist.tail: _*)(v)
            }
            (a2 :+ v) ++ (a3.tail)
          } else {
            a1
          }
        }
        case (a1: JsonObject, i1: String) => {
          if (a1.contains(i1)) {
            if (ilist.size == 1) {
              a1 + (i1 -> v)
            } else {
              a1 + (i1 -> jput(a1(i1), ilist.tail: _*)(v))
            }
          } else {
            a1
          }
        }
        case _ => a
      }
    }
  }

  /**
   *
   * Delete a value within a nested Json value.
   *
   * @param a the input Json value.
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return a copy of the input with the value replaced.
   *
   */
  def jdelete(a: Json, ilist: Any*): Json = {
    if (ilist.size == 0) {
      a
    } else {
      (a, ilist.head) match {
        case (a1: JsonArray, i1: Int) => {
          if (0 <= i1 && i1 < a1.size) {
            val (a2, a3) = a1.splitAt(i1)
            if (ilist.size == 1) {
              a2 ++ (a3.tail)
            } else {
              val v = jdelete(a1(i1), ilist.tail: _*)
              (a2 :+ v) ++ (a3.tail)
            }
          } else {
            a1
          }
        }
        case (a1: JsonObject, i1: String) => {
          if (a1.contains(i1)) {
            if (ilist.size == 1) {
              a1 - i1
            } else {
              a1 + (i1 -> jdelete(a1(i1), ilist.tail: _*))
            }
          } else {
            a1
          }
        }
        case _ => a
      }
    }
  }

  /**
   *
   * Insert a value within a nested Json value.
   *
   * @param a the input Json value.
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *  @param v the new value
   *
   * @return a copy of the input with the value inserted. For JsonArrays the value is inserted before the
   * specified value.
   *
   */
  def jinsert(a: Json, ilist: Any*)(v: Json): Json = {
    if (ilist.size == 0) {
      a
    } else {
      (a, ilist.head) match {
        case (a1: JsonArray, i1: Int) => {
          if (0 <= i1 && i1 <= a1.size) {
            val (a2, a3) = a1.splitAt(i1)
            val v1 = if (ilist.size == 1) {
              v
            } else {
              jput(a1(i1), ilist.tail: _*)(v)
            }
            (a2 :+ v) ++ a3
          } else {
            a1
          }
        }
        case (a1: JsonObject, i1: String) => {
          if (!a1.contains(i1)) {
            if (ilist.size == 1) {
              a1 + (i1 -> v)
            } else {
              a1 + (i1 -> jput(a1(i1), ilist.tail: _*)(v))
            }
          } else {
            a1
          }
        }
        case _ => a
      }
    }
  }

  /**
   *
   * Get the size of a Json value.
   *  - For a Json Object the number of name-value pairs.
   *  - For a Json Array the number of elements.
   *  - For anything else, 0.
   *
   */
  def jsize(j: Json): Int = {
    j match {
      case a1: JsonObject => a1.size
      case a1: JsonArray => a1.size
      case x => 0
    }
  }

  /**
   *
   * Get a string value within a nested Json value.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected string value or "" if not present.
   *
   */
  def jgetString(a: Json, ilist: Any*): String = {
    jget(a, ilist: _*) match {
      case s: String => s
      case x => ""
    }
  }

  /**
   *
   * Get a boolean value within a nested Json value.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected boolean value or false if not present.
   *
   */
  def jgetBoolean(a: Json, ilist: Any*): Boolean = {
    jget(a, ilist: _*) match {
      case b: Boolean => b
      case x => false
    }
  }

  /**
   *
   * Get an integer value within a nested Json value.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected integer value or 0 if not present.
   *
   */
  def jgetInt(a: Json, ilist: Any*): Int = {
    jget(a, ilist: _*) match {
      case l: Long => {
        val i = l.toInt
        if (i == l) { i } else { 0 }
      }
      case i: Int => i
      case x => 0
    }
  }

  /**
   *
   * Get a long value within a nested Json value.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected long value or 0 if not present.
   *
   */
  def jgetLong(a: Json, ilist: Any*): Long = {
    jget(a, ilist: _*) match {
      case l: Long => l
      case i: Int => i
      case x => 0
    }
  }

  /**
   *
   * Get a big decimal value within a nested Json value.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected big decimal value or 0.0 if not present.
   *
   */
  def jgetBigDecimal(a: Json, ilist: Any*): BigDecimal = {
    jget(a, ilist: _*) match {
      case b: BigDecimal => b
      case d: Double => BigDecimal(d)
      case l: Long => l
      case i: Int => i
      case x => 0
    }
  }

  /**
   *
   * Get a double value within a nested Json value.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected double value or 0.0 if not present.
   *
   */
  def jgetDouble(a: Json, ilist: Any*): Double = {
    jget(a, ilist: _*) match {
      case d: Double => d
      case b: BigDecimal => b.toDouble
      case l: Long => l
      case i: Int => i
      case x => 0
    }
  }

  /**
   *
   * Get a JsonArray value within a nested Json value.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected JsonArray value or an empty JsonArray if not present.
   *
   */
  def jgetArray(a: Json, ilist: Any*): JsonArray = {
    jget(a, ilist: _*) match {
      case s: JsonArray => s
      case x => emptyJsonArray
    }
  }

  /**
   *
   * Get a JsonObject value within a nested Json value.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   * @return the selected JsonObject value or an empty JsonObject if not present.
   *
   */
  def jgetObject(a: Json, ilist: Any*): JsonObject = {
    jget(a, ilist: _*) match {
      case m: JsonObject => m
      case x => emptyJsonObject
    }
  }

  /**
   *
   * An extractor for nested Json values.
   *
   * @param ilist a list of values that are either strings or integers
   *  - A string selects the value of a JsonObject name-value pair where
   *    the name equals the string.
   *  - An integer i selects the ith elements of a JsonArray.
   *
   *  @example {{{
   *     val A = jfield("a")
   *     val B = jfield("b")
   *     val C = jfield("c")
   *     jval match {
   *       case a:A & b:B => foo(a,b)
   *       case c:C => bar(c)
   *     }
   *  }}}
   *
   */
  case class jfield(ilist: Any*) {
    def unapply(m: Json) = {
      val result = jget(m, ilist: _*)
      if (result == null) { None } else { Some(result) }
    }
  }

  /**
   *
   * An extractor composition operator.
   *
   * @example {{{
   *     val A = jfield("a")
   *     val B = jfield("b")
   *     val C = jfield("c")
   *     jval match {
   *       case a:A & b:B => foo(a,b)
   *       case c:C => bar(c)
   *     }
   *  }}}
   *
   */
  object & {
    def unapply(a: Any) = Some(a, a)
  }

}