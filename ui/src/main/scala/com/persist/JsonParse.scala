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

/*
 * This class is based on the Json parser given in the Odersky Scala book
 * as modified by Twitter.
 *     https://github.com/stevej/scala-json/
 * 
 * That version was however too slow, so it has been rewritten here with
 * a similar API, but with an emphasis on performance.
 * 
 */

package com.persist

import scala.collection.immutable.HashMap
import JsonOps._
import scala.util.Sorting
import Exceptions._
import scala.annotation.switch

private[persist] object JsonParse {

  // *** Character Kinds

  final type CharKind = Int
  final val Letter = 0
  final val Digit = 1
  final val Minus = 2
  final val Quote = 3
  final val Colon = 4
  final val Comma = 5
  final val Lbra = 6
  final val Rbra = 7
  final val Larr = 8
  final val Rarr = 9
  final val Blank = 10
  final val Other = 11
  final val Eof = 12
  final val Slash = 13

  // *** Token Kinds

  final type TokenKind = Int
  final val ID = 0
  final val STRING = 1
  final val NUMBER = 2
  final val BIGNUMBER = 3
  final val FLOATNUMBER = 4
  final val COLON = 5
  final val COMMA = 6
  final val LOBJ = 7
  final val ROBJ = 8
  final val LARR = 9
  final val RARR = 10
  final val BLANK = 11
  final val EOF = 12

  // *** Character => CharKind Map ***

  final val charKind1 = new Array[CharKind](256)

  for (i <- 0 until 255) {
    charKind1(i) = Other
  }
  for (i <- 'a'.toInt to 'z'.toInt) {
    charKind1(i) = Letter
  }
  for (i <- 'A'.toInt to 'Z'.toInt) {
    charKind1(i) = Letter
  }
  for (i <- '0'.toInt to '9'.toInt) {
    charKind1(i) = Digit
  }
  charKind1('-'.toInt) = Minus
  charKind1(','.toInt) = Comma
  charKind1('"'.toInt) = Quote
  charKind1(':'.toInt) = Colon
  charKind1('{'.toInt) = Lbra
  charKind1('}'.toInt) = Rbra
  charKind1('['.toInt) = Larr
  charKind1(']'.toInt) = Rarr
  charKind1(' '.toInt) = Blank
  charKind1('\t'.toInt) = Blank
  charKind1('\n'.toInt) = Blank
  charKind1('\r'.toInt) = Blank
  charKind1('/'.toInt) = Slash

  // *** Character Escapes

  final val escapeMap1 = HashMap[Int, String](
    '\\'.toInt -> "\\",
    '/'.toInt -> "/",
    '\"'.toInt -> "\"",
    'b'.toInt -> "\b",
    'f'.toInt -> "\f",
    'n'.toInt -> "\n",
    'r'.toInt -> "\r",
    't'.toInt -> "\t")

  def parse(s: String): Json = {
    val jp = new JsonParse(s)
    val result = jp.parse()
    result
  }
}

private[persist] class JsonParse(s: String) {

  // *** Import Shared Data ***

  import JsonParse._
  final private[this] val charKind = charKind1
  final private[this] val escapeMap = escapeMap1

  // *** INPUT STRING ***

  // array faster than accessing string directly using charAt
  //final private[this] val s1 = s.toCharArray()
  final private[this] val size = s.size

  // *** CHARACTERS ***

  final private[this] var pos = 0

  final private[this] var ch: Int = 0
  final private[this] var chKind: CharKind = 0
  final private[this] var chLinePos: Int = 0
  final private[this] var chCharPos: Int = 0

  final private def chNext {
    if (pos < size) {
      //ch = s1(pos).toInt
      ch = s.charAt(pos)
      chKind = if (ch < 255) {
        charKind(ch)
      } else {
        Other
      }
      pos += 1
      if (ch == '\n'.toInt) {
        chLinePos += 1
        chCharPos = 1
      } else {
        chCharPos += 1
      }
    } else {
      ch = -1
      pos = size + 1
      chKind = Eof
    }
  }


  final private def chError(msg: String): Nothing = {
    throw new JsonParseException(msg, s, chLinePos, chCharPos)
  }

  final private def chMark = pos - 1

  final private def chSubstr(first: Int, delta: Int = 0) = {
    s.substring(first, pos - 1 - delta)
  }

  // *** LEXER ***

  final private[this] var tokenKind: TokenKind = BLANK
  final private[this] var tokenValue: String = ""
  final private[this] var linePos = 1
  final private[this] var charPos = 1

  final private def getDigits() {
    while (chKind == Digit) {
      chNext
    }
  }

  final private def handleDigit() {
    val first = chMark
    getDigits()
    val k1 = if (ch == '.'.toInt) {
      chNext
      getDigits()
      BIGNUMBER
    } else {
      NUMBER
    }
    val k2 = if (ch == 'E'.toInt || ch == 'e'.toInt) {
      chNext
      if (ch == '+'.toInt) {
        chNext
      } else if (ch == '-'.toInt) {
        chNext
      }
      getDigits()
      FLOATNUMBER
    } else {
      k1
    }
    tokenKind = k2
    tokenValue = chSubstr(first)
  }

  final private def handleRaw() {
    chNext
    var first = chMark
    var state = 0
    do {
      if (chKind == Eof) chError("EOF encountered in raw string")
      state = if (ch == '}') {
        1
      } else if (ch == '"') {
        if (state == 1) {
          2
        } else if (state == 2) {
          3
        } else {
          0
        }
      } else {
        0
      }
      chNext
    } while (state != 3)
    tokenKind = STRING
    tokenValue = chSubstr(first, 3)
  }

  final private def tokenNext {
    do {
      linePos = chLinePos
      charPos = chCharPos
      val kind: Int = chKind
      (kind: @switch) match {
        case Letter => {
          val first = chMark
          while (chKind == Letter || chKind == Digit) {
            chNext
          }
          tokenKind = ID
          tokenValue = chSubstr(first)
        }
        case Digit => handleDigit()
        case Minus => {
          chNext
          handleDigit()
          tokenValue = "-" + tokenValue
        }
        case Quote => {
          var sb: StringBuilder = null
          chNext
          var first = chMark
          while (ch != '"'.toInt && ch >= 32) {
            if (ch == '\\'.toInt) {
              if (sb == null) sb = new StringBuilder(50)
              sb.append(chSubstr(first))
              chNext
              escapeMap.get(ch) match {
                case Some(s) => {
                  sb.append(s)
                  chNext
                }
                case None => {
                  if (ch != 'u'.toInt) chError("Illegal escape")
                  chNext
                  var code = 0
                  for (i <- 1 to 4) {
                    val ch1 = ch.toChar.toString
                    val i = "0123456789abcdef".indexOf(ch1.toLowerCase)
                    if (i == -1) chError("Illegal hex character")
                    code = code * 16 + i
                    chNext
                  }
                  sb.append(code.toChar.toString)
                }
              }
              first = chMark
            } else {
              chNext
            }
          }
          if (ch != '"') chError("Unexpected string character:" + ch.toChar)
          val s1 = chSubstr(first)
          val s2 = if (sb == null) s1
          else {
            sb.append(s1)
            sb.toString
          }
          tokenKind = STRING
          tokenValue = s2
          chNext
          if (s2.length() == 0 && ch == '{') {
            handleRaw()
          }
        }
        case Colon => {
          chNext
          tokenKind = COLON
          tokenValue = ""
        }
        case Comma => {
          chNext
          tokenKind = COMMA
          tokenValue = ""
        }
        case Lbra => {
          chNext
          tokenKind = LOBJ
          tokenValue = ""
        }
        case Rbra => {
          chNext
          tokenKind = ROBJ
          tokenValue = ""
        }
        case Larr => {
          chNext
          tokenKind = LARR
          tokenValue = ""
        }
        case Rarr => {
          chNext
          tokenKind = RARR
          tokenValue = ""
        }
        case Blank => {
          do {
            chNext
          } while (chKind == Blank)
          tokenKind = BLANK
          tokenValue = ""
        }
        case Other => chError("Unexpected character")
        case Eof => {
          chNext
          tokenKind = EOF
          tokenValue = ""
        }
        case Slash => {
          val first = chMark
          if (chKind != Slash) chError("Expecting Slash")
          do {
            chNext
          } while (ch != '\n' && chKind != Eof)
          tokenKind = BLANK
          tokenValue = ""
        }
      }
    } while (tokenKind == BLANK)
  }

  final private def tokenError(msg: String): Nothing = {
    throw new JsonParseException(msg, s, linePos, charPos)
  }

  // *** PARSER ***

  final private def handleEof() {
    tokenError("Unexpected eof")
  }

  final private def handleUnexpected() {
    tokenError("Unexpected input")
  }

  final private def handleArray() = {
    tokenNext
    var result = List[Json]()
    while (tokenKind != RARR) {
      val t = getJson
      result = t +: result
      if (tokenKind == COMMA) {
        tokenNext
      } else if (tokenKind == RARR) {
      } else {
        tokenError("Expecting , or ]")
      }
    }
    tokenNext
    result.reverse
  }

  final private[this] val emptyMap = HashMap[String, Json]()

  final private def handleObject() = {
    tokenNext
    var result = emptyMap
    while (tokenKind != ROBJ) {
      if (tokenKind != STRING && tokenKind != ID) tokenError("Expecting string or name")
      val name = tokenValue
      tokenNext
      if (tokenKind != COLON) tokenError("Expecting :")
      tokenNext
      val t = getJson
      result += (name -> t)
      if (tokenKind == COMMA) {
        tokenNext
      } else if (tokenKind == ROBJ) {
      } else {
        tokenError("Expecting , or }")
      }
    }
    tokenNext
    result
  }

  final private def getJson(): Json = {
    val kind: Int = tokenKind
    val result = (kind: @switch) match {
      case ID => {
        val result = if (tokenValue == "true") {
          true
        } else if (tokenValue == "false") {
          false
        } else if (tokenValue == "null") {
          null
        } else {
          tokenError("Not true, false, or null")
        }
        tokenNext
        result
      }
      case STRING => {
        val result = tokenValue
        tokenNext
        result
      }
      case NUMBER => {
        val v = try {
          tokenValue.toLong
        } catch {
          case _: Throwable => tokenError("Bad integer")
        }
        tokenNext
        val r: Json = if (v >= Int.MinValue && v <= Int.MaxValue) v.toInt else v
        r
      }
      case BIGNUMBER => {
        val v = try {
          BigDecimal(tokenValue)
        } catch {
          case _: Throwable => tokenError("Bad decimal number")
        }
        tokenNext
        v
      }
      case FLOATNUMBER => {
        val v = try {
          tokenValue.toDouble
        } catch {
          case _: Throwable => tokenError("Bad double")
        }
        tokenNext
        v
      }
      case COLON => handleUnexpected()
      case COMMA => handleUnexpected()
      case LOBJ => handleObject()
      case ROBJ => handleUnexpected()
      case LARR => handleArray()
      case RARR => handleUnexpected()
      case EOF => handleEof()
    }
    result
  }

  final def parse(): Json = {
    chNext
    tokenNext
    val result = getJson
    if (tokenKind != EOF) tokenError("Excess input")
    result
  }
}

private[persist] object JsonUnparse {

  private def quotedChar(codePoint: Int) = {
    codePoint match {
      case c if c > 0xffff =>
        val chars = Character.toChars(c)
        "\\u%04x\\u%04x".format(chars(0).toInt, chars(1).toInt)
      case c if (c > 0x7e || c < 0x20) => "\\u%04x".format(c.toInt)
      case c => c.toChar
    }
  }

  /**
   * Quote a string according to "JSON rules".
   */
  private def quote(s: String) = {
    val charCount = s.codePointCount(0, s.length)
    "\"" + 0.to(charCount - 1).map {
      idx =>
        s.codePointAt(s.offsetByCodePoints(0, idx)) match {
          case 0x0d => "\\r"
          case 0x0a => "\\n"
          case 0x09 => "\\t"
          case 0x22 => "\\\""
          case 0x5c => "\\\\"
          //case 0x2f => "\\/" // to avoid sending "</"
          case c => quotedChar(c)
        }
    }.mkString("") + "\""
  }

  def compact(obj: Json): String = {
    val sb = new StringBuilder(200)
    def compact1(obj1: Json) {
      obj1 match {
        case s: String => sb.append(quote(s))
        case null => sb.append("null")
        case x: Boolean => sb.append(x.toString)
        case x: Double => sb.append("%1$E".format(x))
        case x: Number => sb.append(x.toString)
        case list: Seq[_] => {
          if (list.headOption == None) {
            sb.append("[]")
          } else {
            var sep = "["
            for (elem <- list) {
              sb.append(sep)
              compact1(elem)
              sep = ","
            }
            sb.append("]")
          }
        }
        case m: scala.collection.Map[_, _] => {
          val m2 = m.asInstanceOf[scala.collection.Map[String, Json]].iterator.toList
          val m1 = Sorting.stableSort[(String, Json), String](m2, {
            case (k, v) => k
          })
          if (m1.size == 0) {
            sb.append("{}")
          } else {
            var sep = "{"
            for ((name, elem) <- m1) {
              sb.append(sep)
              sb.append(quote(name))
              sb.append(":")
              compact1(elem)
              sep = ","
            }
            sb.append("}")
          }
        }
        case x => throw new SystemException("JsonUnparse", JsonObject("msg" -> "bad json value", "value" -> x.toString()))
      }
    }
    compact1(obj)
    sb.toString
  }

  private def isMultiLine(s: String): Boolean = s.indexOf("\n") >= 0

  private def doIndent(s: String, indent: Int, first: String = ""): String = {
    val space = " " * indent
    if (isMultiLine(s)) {
      val parts = s.split("\n")
      val head = parts.head
      val tail = parts.tail
      val indent1 = first.size + indent
      val space1 = " " * indent1
      val head1 = space + first + head
      val tail1 = tail.map(part => space1 + part)
      val seq1 = head1 +: tail1
      seq1.mkString("\n")
    } else {
      space + first + s
    }
  }

  private def wrap(first: String, sep: String, last: String, indent: Int, seq: Seq[String]): String = {
    if (seq.size == 0) {
      doIndent(first + last, indent)
    } else {
      val indent1 = first.size + indent
      val head = seq.head
      val tail = seq.tail
      val head1 = doIndent(head, indent, first)
      val tail1 = tail.map(part => doIndent(part, indent1))
      val seq1 = head1 +: tail1
      seq1.mkString(",\n") + "\n" + doIndent(last, indent)
    }
  }

  private[this] val WIDTH = 50
  private[this] val COUNT = 6

  private def split(s: Seq[String]): Boolean = {
    s.size > COUNT ||
      s.map(isMultiLine(_)).fold(false) {
        _ || _
      } ||
      s.map(_.size).fold(0)(_ + _) + s.size + 2 > WIDTH
  }

  /**
   * Returns a pretty JSON representation of the given object
   */
  def pretty(obj: Json, indent: Int = 0): String = {
    obj match {
      case null => doIndent("null", indent)
      case x: Boolean => doIndent(x.toString, indent)
      case x: Double => doIndent("%1$E".format(x), indent)
      case x: Number => doIndent(x.toString, indent)
      case array: Array[Json] => pretty(array.toList, indent)
      case list: Seq[_] =>
        val strings = list.map(pretty(_))
        if (!split(strings)) {
          doIndent("[" + strings.mkString(",") + "]", indent)
        } else {
          wrap("[", ",", "]", indent, strings)
        }
      case map: scala.collection.Map[_, _] =>
        val seq2 = Sorting.stableSort[(Any, Json), String](map.iterator.toList, {
          case (k, v) => k.toString
        })
        val strings = seq2.map {
          case (k, v) => {
            val v1 = pretty(v)
            val label = quote(k.toString) + ":"
            if (isMultiLine(v1) || label.size + v1.size > WIDTH) {
              label + "\n" + doIndent(v1, 2)
            } else {
              label + v1
            }
          }
        }
        if (!split(strings)) {
          doIndent("{" + strings.mkString(",") + "}", indent)
        } else {
          wrap("{", ",", "}", indent, strings)
        }
      case s: String => doIndent(quote(s), indent)
      case x => {
        throw new SystemException("JsonUnparse", JsonObject("msg" -> "bad json value", "value" -> x.toString()))
      }
    }
  }

}
