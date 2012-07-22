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
import com.twitter.json.Json.parse
import com.twitter.json.Json.build
import com.twitter.json.Json.compact
import com.twitter.json.Json.pretty
import java.net.URLEncoder
import java.net.URLDecoder

/**
 *
 * Provides types and functions for working with the OStore Json types.
 *
 * In OStore, Json is represented by immutable Scala types.
 * Instead of having separate Json types, type aliases are
 * defined for Json forms.
 *
 * Scala types used for Json are
 *
 *  - Json Object. Immutable Map[String,Json]
 *  - Json Array. Immutable Seq[Json]
 *  - Json String. String
 *  - Json Boolean. Boolean
 *  - Json Number. Int, Long, BigDecimal
 *  - Json Null. Null
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
   * A type alias for OStore Json keys.
   * JsonKey values should be restricted to
   * permitted OStore key forms.
   *
   *  - String.
   *  - Number (restricted to integers that can fit in a Long).
   *  - Array of JsonKeys.
   */
  type JsonKey = Json

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
   */
  val emptyJsonArray = JsonArray()

  /**
   *
   * A Json parser.
   */
  def Json(s: String): Json = {
    //parse(s)
    com.persist.JsonParse.parse(s)
  }

  /**
   *
   * A Json unparser. It produces the most compact single line string form.
   *
   */
  def Compact(j: Json): String = {
    //build(j).toString()
    compact(j)
  }

  /**
   *
   * A Json unparser. It produces a multiple line form
   * designed for human readability.
   *
   */
  def Pretty(j: Json): String = {
    pretty(j).toString()
  }

  /**
   *
   * Get a value within a nested Json value.
   *
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
   * Get the size of a Json value.
   *  - For a Json Object the number of name-value pairs.
   *  - For a Json Array the number of elements.
   *  - For anything else, 0.
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
      case l:Long => l
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
   */
  object & {
    def unapply(a: Any) = Some(a, a)
  }

  /**
   *
   * Compares two JsonKeys.
   *
   * @param a the first key.
   * @param b the second key.
   *
   * @return
   *  - 0 if the two keys are equal.
   *  - <0 if a is less than b in key order.
   *  - >0 if a is greater than b in key order.
   *
   */
  def keyCompare(a: Json, b: Json): Int = {
    keyEncode(a).compareTo(keyEncode(b))
  }

  /**
   *
   * Compares two JsonKeys for equality.
   *
   * @param a the first key.
   * @param b the second key.
   *
   * @return true if the two keys are equal.
   *
   */
  def keyEq(a: Json, b: Json): Boolean = {
    keyCompare(a, b) == 0
  }

  /**
   *
   * Compares two JsonKeys.
   *
   * @param a the first key.
   * @param b the second key.
   *
   * @return true if a is less than b in key ordering.
   *
   */
  def keyLs(a: Json, b: Json): Boolean = {
    keyCompare(a, b) < 0
  }

  /**
   *
   * Determines if one JsonKey is a prefix of another JsonKey.
   *
   * @param a the first key
   * @param b the second key
   *
   * @return true if a is a prefix of b.
   *
   */
  def keyPrefix(a: JsonKey, b: JsonKey): Boolean = {
    val a1 = keyEncode(a)
    val b1 = keyEncode(b)
    a1.size != b1.size && b1.startsWith(a1)
  }

  private val poss = "bcdefghijklmnopqrstuvwxy"
  private val negs = "YXWVUTSRQPONMLKJIHGFEDCB"

  private def comp(v: Long): Long = {
    var cover = 10
    while (v > cover) {
      cover = 10 * cover
    }
    (cover - v) + (cover / 10) - 1
  }

  private def keyEncode1(j: JsonKey, last: Boolean): String = {
    j match {
      case s: String => {
        for (i <- 0 to s.length() - 1) {
          val ch = s(i)
          if (ch == '\u0000' || ch == '\uffff') {
            throw new Exception("illegal character in key string")
          }
        }
        // note last is an open prefix
        "*" + s + (if (last) { "" } else { "\u0000" })
      }
      case v: Long => {
        "#" +
          (if (v == Long.MinValue) {
            "A"
          } else if (v == Long.MaxValue) {
            "z"
          } else if (v < 0) {
            val v1 = comp(-v)
            val s = "" + v1
            val len = s.length()
            val pre = negs.substring(len - 1, len)
            pre + s
          } else if (v == 0) {
            "a"
          } else {
            // v > 0
            val s = "" + v
            val len = s.length()
            val pre = poss.substring(len - 1, len)
            pre + s
          })

      }
      case v: Int => keyEncode1(v.toLong, last)
      case a: JsonArray => {
        var result = "["
        var i = 0
        for (j1 <- a) {
          val s = keyEncode1(j1, false)
          result = result + s
          i += 1
        }
        if (!last) result = result + "]"
        result
      }
      case x => {
        throw new Exception("bad key form:" + x)
      }
    }
  }

  private[persist] def keyEncode(j: JsonKey): String = {
    keyEncode1(j, true)
  }

  private def keyDecode1(s: String, i: Int): (JsonKey, Int) = {
    val len = s.length()
    val pos0 = i + 1
    val code = s(i)
    if (code == '#') {
      val icode = s(pos0)
      var pos = pos0 + 1
      val vv = if (icode == 'A') {
        Long.MinValue
      } else if (icode == 'z') {
        Long.MaxValue
      } else if (icode == 'a') {
        0
      } else {
        val neg = icode <= 'Z'
        var v = 0L
        while (pos < len && '0' <= s(pos) && s(pos) <= '9') {
          pos = pos + 1
        }
        val sn = s.substring(pos0 + 1, pos)
        v = sn.toLong
        if (neg) { v = -comp(v) }
        v
      }
      (vv, pos)
    } else if (code == '*') {
      var pos = pos0
      while (pos < len && s(pos) != '\u0000') {
        pos = pos + 1
      }
      val s1 = s.substring(pos0, pos)
      if (pos < len) pos = pos + 1 // skip 0
      (s1, pos)
    } else if (code == '[') {
      var result = JsonArray()
      var pos = pos0
      var done = false;
      while (!done && pos < len) {
        if (pos < len && s(pos) == ']') {
          pos = pos + 1
          done = true
        } else {
          val (j, pos1) = keyDecode1(s, pos)
          result = j +: result
          pos = pos1
        }
      }
      (result.reverse, pos)
    } else {
      throw new Exception("bad key item code")
    }
  }

  private def keyDecode0(s: String): JsonKey = {
    val len = s.length()
    val (j, pos) = keyDecode1(s, 0)
    if (pos != len) {
      throw new Exception("excess key characters")
    }
    j
  }

  private[persist] def keyDecode(s: String): JsonKey = {
    try {
      if (s == "") {
          false  // Min
      } else if (s == "\uFFFF") {
          true   // Max
      } else {
        keyDecode0(s)
      }
    } catch {
      case ex => {
        // internal error
        println("Json key decode failed: " + ex.toString())
        ex.printStackTrace()
        s
      }
    }
  }

  /**
   *
   * An encoder for REST key forms.
   * Converts an OStore key to a string form that
   * can be used as a key within the query part of
   * a url for the OStore REST api.
   *
   */
  def keyUriEncode(j: JsonKey): String = {
    j match {
      case s: String => {
        val s1 = URLEncoder.encode(s, "UTF-8")
        if (s.length() > 0 && '0' <= s(0) && s(0) <= '9') {
          "\"" + s1 + "\""
        } else {
          s1
        }
      }
      case v: Long => {
        v.toString()
      }
      case v: Int => {
        v.toString()
      }
      case j: JsonArray => {
        var result = "("
        val lasti = jsize(j) - 1
        var i = 0
        for (j1 <- j) {
          val s = keyUriEncode(j1)
          result = result + s
          if (i != lasti) result = result + ","
          i += 1
        }
        result = result + ")"
        result

      }
      case x => {
        throw new Exception("bad key form")

      }
    }
  }

  private def keyUriDecode2(s: String): JsonKey = {
    try {
      val v = java.lang.Long.decode(s)
      v
    } catch {
      case ex => {
        val s1 = if (s.length() >= 2 && s(0) == '"') {
          s.substring(1, s.length() - 1)
        } else {
          s
        }
        val s2 = URLDecoder.decode(s1, "UTF-8")
        s2
      }
    }
  }

  private def getUriItem(s: String, pos0: Int): (String, Int) = {
    var pos = pos0
    val len = s.length()
    while (pos < len && s(pos) != ',' && s(pos) != ')') {
      pos = pos + 1
    }
    (s.substring(pos0, pos), pos)
  }

  private def keyUriDecode1(s: String, pos0: Int): (JsonKey, Int) = {
    val len = s.length()
    var pos = pos0
    if (s(pos) == '(') {
      pos = pos + 1
      var a = JsonArray()
      while (pos < len && s(pos) != ')') {
        val (s1, pos1) = keyUriDecode1(s, pos)
        a = s1 +: a
        pos = pos1
        if (pos < len && s(pos) == ',') pos = pos1 + 1
      }
      if (pos < len && s(pos) == ')') pos = pos + 1
      (a.reverse, pos)
    } else {
      val (s1, pos1) = getUriItem(s, pos)
      (keyUriDecode2(s1), pos1)
    }
  }

  /**
   *
   * An decoder for REST key forms.
   * Converts a key within the query part of
   * a url for the OStore REST api to a JsonKey.
   *
   */
  def keyUriDecode(s: String): JsonKey = {
    val (s1, pos1) = keyUriDecode1(s, 0)
    s1
  }
}