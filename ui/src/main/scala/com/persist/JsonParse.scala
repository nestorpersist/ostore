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

private[persist] object JsonParse {
    type TokenKind = Int
    private[this] val ID = 0
    private[this] val STRING = 1
    private[this] val NUMBER = 2
    private[this] val BIGNUMBER = 3
    private[this] val FLOATNUMBER = 4
    private[this] val COLON = 5
    private[this] val COMMA = 6
    private[this] val LOBJ = 7
    private[this] val ROBJ = 8
    private[this] val LARR = 9
    private[this] val RARR = 10
    private[this] val BLANK = 11
    private[this] val EOF = 12
    private[this] val TokenKindSize = 13
    
    private object lexer {

    type CharKind = Int
    private[this] val Letter = 0
    private[this] val Digit = 1
    private[this] val Minus = 2
    private[this] val Quote = 3
    private[this] val Colon = 4
    private[this] val Comma = 5
    private[this] val Lbra = 6
    private[this] val Rbra = 7
    private[this] val Larr = 8
    private[this] val Rarr = 9
    private[this] val Blank = 10
    private[this] val Other = 11
    private[this] val Eof = 12
    private[this] val Slash = 13
    private[this] val CharKindSize = 14

    private[this] var charKind = new Array[CharKind](256)
    for (i <- 0 until 255) {
      charKind(i) = Other
    }
    for (i <- 'a'.toInt to 'z'.toInt) {
      charKind(i) = Letter
    }
    for (i <- 'A'.toInt to 'Z'.toInt) {
      charKind(i) = Letter
    }
    for (i <- '0'.toInt to '9'.toInt) {
      charKind(i) = Digit
    }
    charKind('-'.toInt) = Minus
    charKind(','.toInt) = Comma
    charKind('"'.toInt) = Quote
    charKind(':'.toInt) = Colon
    charKind('{'.toInt) = Lbra
    charKind('}'.toInt) = Rbra
    charKind('['.toInt) = Larr
    charKind(']'.toInt) = Rarr
    charKind(' '.toInt) = Blank
    charKind('\t'.toInt) = Blank
    charKind('\n'.toInt) = Blank
    charKind('\r'.toInt) = Blank
    charKind('/'.toInt) = Slash

    private[this] val charAction = new Array[(Chars) => (TokenKind, String)](CharKindSize)
    charAction(Letter) = handleLetter
    charAction(Digit) = handleDigit
    charAction(Minus) = handleMinus
    charAction(Quote) = handleQuote
    charAction(Colon) = handleSimple(COLON)
    charAction(Comma) = handleSimple(COMMA)
    charAction(Lbra) = handleSimple(LOBJ)
    charAction(Rbra) = handleSimple(ROBJ)
    charAction(Larr) = handleSimple(LARR)
    charAction(Rarr) = handleSimple(RARR)
    charAction(Blank) = handleBlank
    charAction(Other) = handleUnexpected
    charAction(Eof) = handleSimple(EOF)
    charAction(Slash) = handleSlash

    private def escapeMap = HashMap[Int, String](
      '\\'.toInt -> "\\",
      '/'.toInt -> "/",
      '\"'.toInt -> "\"",
      'b'.toInt -> "\b",
      'f'.toInt -> "\f",
      'n'.toInt -> "\n",
      'r'.toInt -> "\r",
      't'.toInt -> "\t")

    private def handleRaw(chars: Chars) = {
      var ch = chars.next
      var first = chars.mark
      var state = 0
      do {
        if (chars.getKind == Eof) chars.error("EOF encountered in raw string")
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
        ch = chars.next
      } while (state != 3)
      val s = chars.substr(first, 3)
      (STRING, s)
    }

    private def handleQuote(chars: Chars) = {
      var sb: StringBuilder = null
      var ch = chars.next
      var first = chars.mark
      while (ch != '"'.toInt && ch >= 32) {
        if (ch == '\\'.toInt) {
          if (sb == null) sb = new StringBuilder(50)
          sb.append(chars.substr(first))
          ch = chars.next
          escapeMap.get(ch) match {
            case Some(s) => {
              sb.append(s)
              ch = chars.next
            }
            case None => {
              if (ch != 'u'.toInt) chars.error("Illegal escape")
              ch = chars.next
              var code = 0
              for (i <- 1 to 4) {
                val ch1 = ch.toChar.toString
                val i = "0123456789abcdef".indexOf(ch1.toLowerCase)
                if (i == -1) chars.error("Illegal hex character")
                code = code * 16 + i
                ch = chars.next
              }
              sb.append(code.toChar.toString)
            }
          }
          first = chars.mark
        } else {
          ch = chars.next
        }
      }
      if (ch != '"') chars.error("Unexpected string character:" + ch.toChar)
      val s1 = chars.substr(first)
      val s2 = if (sb == null) s1 else {
        sb.append(s1)
        sb.toString
      }
      val result = (STRING, s2)
      ch = chars.next
      if (s2.length() == 0 && ch == '{') {
        handleRaw(chars)
      } else {
        result
      }
    }

    private def handleLetter(chars: Chars) = {
      val first = chars.mark
      while ({ val kind = chars.getKind; kind == Letter || kind == Digit }) {
        chars.next
      }
      val s = chars.substr(first)
      (ID, s)
    }

    private def getDigits(chars: Chars) {
      while (chars.getKind == Digit) {
        chars.next
      }
    }

    private def handleDigit(chars: Chars) = {
      val first = chars.mark
      getDigits(chars)
      var ch = chars.getCh
      val k1 = if (ch == '.'.toInt) {
        ch = chars.next
        getDigits(chars)
        ch = chars.getCh
        BIGNUMBER
      } else {
        NUMBER
      }
      val k2 = if (ch == 'E'.toInt || ch == 'e'.toInt) {
        ch = chars.next
        if (ch == '+'.toInt) {
          ch = chars.next
        } else if (ch == '-'.toInt) {
          ch = chars.next
        }
        getDigits(chars)
        FLOATNUMBER
      } else {
        k1
      }
      (k2, chars.substr(first))
    }

    private def handleMinus(chars: Chars) = {
      chars.next
      val (kind, v) = handleDigit(chars)
      (kind, "-" + v)
    }

    private def handleSimple(t: TokenKind)(chars: Chars) = {
      chars.next
      (t, "")
    }

    private def handleBlank(chars: Chars) = {
      do {
        chars.next
      } while (chars.getKind == Blank)
      (BLANK, "")
    }

    private def handleSlash(chars: Chars) = {
      val first = chars.mark
      if (chars.getKind != Slash) chars.error("Expecting Slash")
      var ch = chars.getCh
      do {
        ch = chars.next
      } while (ch != '\n' && chars.getKind != Eof)
      (BLANK, "")
    }

    private def handleUnexpected(chars: Chars) = {
      chars.error("Unexpected character")
    }

    private class Chars(val s: String) {
      private[this] val s1 = s.toCharArray() // faster than accessing string directly
      private[this] var pos = 0
      private[this] val size = s.size
      private[this] var ch: Int = 0
      def getCh = ch
      private[this] var kind: CharKind = Other
      def getKind = kind
      private[this] var linePos: Int = 1
      def getLinePos = linePos
      private[this] var charPos = 0
      def getCharPos = charPos
      def next: Int = {
        if (pos < size) {
          ch = s1(pos).toInt
          kind = if (ch < 255) {
            charKind(ch)
          } else {
            Other
          }
          pos += 1
          if (ch == '\n'.toInt) {
            linePos += 1
            charPos = 1
          } else {
            charPos += 1
          }
        } else {
          ch = -1
          pos = size + 1
          kind = Eof
        }
        ch
      }
      def error(msg: String): Nothing = {
        throw new JsonParseException(msg, s, linePos, charPos)
      }
      def mark = pos - 1
      def substr(first: Int, delta: Int = 0) = {
        s.substring(first, pos - 1 - delta)
      }
      next
    }

    class Tokens(val s: String) {
      private[this] val chars = new Chars(s)
      private[this] var value: String = ""
      def getValue = value
      private[this] var kind: TokenKind = BLANK
      def getKind = kind
      private[this] var linePos = 1
      private[this] var charPos = 1
      def next: TokenKind = {
        do {
          linePos = chars.getLinePos
          charPos = chars.getCharPos
          val (k, v) = charAction(chars.getKind)(chars)
          kind = k
          value = v
        } while (kind == BLANK)
        kind
      }
      def error(msg: String): Nothing = {
        throw new JsonParseException(msg, s, linePos, charPos)
      }
      next
    }

  }
  import lexer._
  
  private[this] val tokenAction = new Array[(Tokens) => Json](TokenKindSize)
  tokenAction(ID) = handleId
  tokenAction(STRING) = handleString
  tokenAction(NUMBER) = handleNumber
  tokenAction(BIGNUMBER) = handleBigNumber
  tokenAction(FLOATNUMBER) = handleFloatNumber
  tokenAction(COLON) = handleUnexpected
  tokenAction(COMMA) = handleUnexpected
  tokenAction(LOBJ) = handleObject
  tokenAction(ROBJ) = handleUnexpected
  tokenAction(LARR) = handleArray
  tokenAction(RARR) = handleUnexpected
  tokenAction(EOF) = handleEof

  private def handleId(tokens: Tokens) = {
    val value = tokens.getValue
    val result = if (value == "true") {
      true
    } else if (value == "false") {
      false
    } else if (value == "null") {
      null
    } else {
      tokens.error("Not true, false, or null")
    }
    tokens.next
    result
  }

  private def handleString(tokens: Tokens) = {
    val result = tokens.getValue
    tokens.next
    result
  }

  private def handleNumber(tokens: Tokens) = {
    val v = try {
      tokens.getValue.toLong
    } catch {
      case _ => tokens.error("Bad integer")
    }
    tokens.next
    val r: Json = if (v >= Int.MinValue && v <= Int.MaxValue) v.toInt else v
    r
  }

  private def handleBigNumber(tokens: Tokens) = {
    val v = try {
      BigDecimal(tokens.getValue)
    } catch {
      case _ => tokens.error("Bad decimal number")
    }
    tokens.next
    v
  }

  private def handleFloatNumber(tokens: Tokens) = {
    val v = try {
      tokens.getValue.toDouble
    } catch {
      case _ => tokens.error("Bad double")
    }
    tokens.next
    v
  }

  private def handleArray(tokens: Tokens) = {
    var kind = tokens.next
    var result = List[Json]()
    while (kind != RARR) {
      val t = getJson(tokens)
      kind = tokens.getKind
      result = t +: result
      if (kind == COMMA) {
        kind = tokens.next
      } else if (kind == RARR) {
      } else {
        tokens.error("Expecting , or ]")
      }
    }
    tokens.next
    result.reverse
  }

  private def handleObject(tokens: Tokens) = {
    var kind = tokens.next
    var result = collection.mutable.HashMap[String, Json]()
    while (kind != ROBJ) {
      if (kind != STRING && kind != ID) tokens.error("Expecting string or name")
      val name = tokens.getValue
      kind = tokens.next
      if (kind != COLON) tokens.error("Expecting :")
      kind = tokens.next
      val t = getJson(tokens)
      kind = tokens.getKind
      result += (name -> t)
      if (kind == COMMA) {
        kind = tokens.next
      } else if (kind == ROBJ) {
      } else {
        tokens.error("Expecting , or }")
      }
    }
    tokens.next
    result.toMap // This is really slow. Unclear how to speed it up
  }

  private def handleEof(tokens: Tokens) {
    tokens.error("Unexpected eof")
  }
  private def handleUnexpected(tokens: Tokens) {
    tokens.error("Unexpected input")
  }

  private def getJson(tokens: Tokens) = {
    tokenAction(tokens.getKind)(tokens)
  }

  def parse(s: String): Json = {
    val tokens = new Tokens(s)
    val result = getJson(tokens)
    if (tokens.getKind != EOF) tokens.error("Excess input")
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
    "\"" + 0.to(charCount - 1).map { idx =>
      s.codePointAt(s.offsetByCodePoints(0, idx)) match {
        case 0x0d => "\\r"
        case 0x0a => "\\n"
        case 0x09 => "\\t"
        case 0x22 => "\\\""
        case 0x5c => "\\\\"
        case 0x2f => "\\/" // to avoid sending "</"
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
        case m: Map[String, _] => {
          val m2 = m.iterator.toList
          val m1 = Sorting.stableSort[(String, Json), String](m2, { case (k, v) => k })
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
      s.map(isMultiLine(_)).fold(false) { _ || _ } ||
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
        val seq2 = Sorting.stableSort[(Any, Json), String](map.iterator.toList, { case (k, v) => k.toString })
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