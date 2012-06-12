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
import com.twitter.json.Json.pretty
import java.net.URLEncoder
import java.net.URLDecoder

object JsonOps {

  type Json = Any
  type JsonKey = Json

  type JsonObject = Map[String, Json]
  type JsonPair = (String, Json)
  def JsonObject(i: JsonPair*): JsonObject = new HashMap[String, Json]() ++ i
  val emptyJsonObject = JsonObject()

  type JsonArray = Seq[Json]
  def JsonArray(i: Json*): JsonArray = List() ++ i
  val emptyJsonArray = JsonArray()

  def Json(s: String): Json = {
    parse(s)
  }

  def Compact(j: Json): String = {
    build(j).toString()
  }

  def Pretty(j: Json): String = {
    pretty(j).toString()
  }

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

  def jsize(a: Json): Int = {
    a match {
      case a1: JsonObject => a1.size
      case a1: JsonArray => a1.size
      case x => 0
    }
  }

  def jgetString(a: Json, ilist: Any*): String = {
    jget(a, ilist: _*) match {
      case s: String => s
      case x => ""
    }
  }

  def jgetBoolean(a: Json, ilist: Any*): Boolean = {
    jget(a, ilist: _*) match {
      case b: Boolean => b
      case x => false
    }
  }

  def jgetInt(a: Json, ilist: Any*): Int = {
    jget(a, ilist: _*) match {
      case i: Int => i
      case x => 0
    }
  }

  def jgetLong(a: Json, ilist: Any*): Long = {
    jget(a, ilist: _*) match {
      case l: Long => l
      case i: Int => i
      case x => 0
    }
  }

  def jgetArray(a: Json, ilist: Any*): JsonArray = {
    jget(a, ilist: _*) match {
      case s: JsonArray => s
      case x => emptyJsonArray
    }
  }

  def jgetObject(a: Json, ilist: Any*): JsonObject = {
    jget(a, ilist: _*) match {
      case m: JsonObject => m
      case x => emptyJsonObject
    }
  }

  case class jfield(i: Any) {
    def unapply(m: Json) = {
      val result = jget(m, i)
      if (result == null) { None } else { Some(result) }
    }
  }

  object & {
    def unapply(a: Any) = Some(a, a)
  }

  def keyCompare(a: Json, b: Json): Int = {
    keyEncode(a).compareTo(keyEncode(b))
  }

  def keyEq(a: Json, b: Json): Boolean = {
    keyCompare(a, b) == 0
  }

  def keyLs(a: Json, b: Json): Boolean = {
    keyCompare(a, b) < 0
  }

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

  def keyEncode(j: JsonKey): String = {
    keyEncode1(j, true)
  }

  private def keyDecode1(s: String, i: Integer): (JsonKey, Integer) = {
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

  def keyDecode0(s: String): JsonKey = {
    val len = s.length()
    val (j, pos) = keyDecode1(s, 0)
    if (pos != len) {
      throw new Exception("excess key characters")
    }
    j
  }

  def keyDecode(s: String): JsonKey = {
    try {
      keyDecode0(s)
    } catch {
      case ex => {
        // internal error
        println("Json key decode failed: " + ex.toString())
        ex.printStackTrace()
        s
      }
    }
  }

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
  def keyUriDecode(s: String): JsonKey = {
    val (s1, pos1) = keyUriDecode1(s, 0)
    s1
  }
}