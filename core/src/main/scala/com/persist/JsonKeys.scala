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

import java.net.URLEncoder
import java.net.URLDecoder
import JsonOps._

/**
 *
 * Provides types and functions for working with the OStore Json key types.
 *
 */
object JsonKeys {

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
        false // Min
      } else if (s == "\uFFFF") {
        true // Max
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