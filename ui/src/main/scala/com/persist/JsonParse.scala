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

  private object lexer {

    private object CharKinds extends Enumeration {
      type CharKind = Value
      val Letter, Digit, Minus, Quote, Colon, Comma, Lbra, Rbra, Larr, Rarr, Blank, Other, Eof = Value
    }
    import CharKinds._

    object TokenKinds extends Enumeration {
      type TokenKind = Value
      val ID, STRING, NUMBER, BIGNUMBER, COLON, COMMA, LOBJ, ROBJ, LARR, RARR, BLANK, EOF = Value
    }
    import TokenKinds._

    private var charKind = new Array[CharKind](256)
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

    private val charAction = new Array[(Chars) => (TokenKind, String)](CharKinds.values.size)
    charAction(Letter.id) = handleLetter
    charAction(Digit.id) = handleDigit
    charAction(Minus.id) = handleMinus
    charAction(Quote.id) = handleQuote
    charAction(Colon.id) = handleSimple(COLON)
    charAction(Comma.id) = handleSimple(COMMA)
    charAction(Lbra.id) = handleSimple(LOBJ)
    charAction(Rbra.id) = handleSimple(ROBJ)
    charAction(Larr.id) = handleSimple(LARR)
    charAction(Rarr.id) = handleSimple(RARR)
    charAction(Blank.id) = handleBlank
    charAction(Other.id) = handleUnexpected
    charAction(Eof.id) = handleSimple(EOF)

    private def escapeMap = HashMap[Int, String](
      '\\'.toInt -> "\\",
      '/'.toInt -> "/",
      '\"'.toInt -> "\"",
      'b'.toInt -> "\b",
      'f'.toInt -> "\f",
      'n'.toInt -> "\n",
      'r'.toInt -> "\r",
      't'.toInt -> "\t")

    private def handleQuote(chars: Chars) = {
      var sb: StringBuilder = null
      chars.next
      var first = chars.mark
      while (chars.ch != '"'.toInt && chars.ch >= 32) {
        if (chars.ch == '\\'.toInt) {
          if (sb == null) sb = new StringBuilder(50)
          sb.append(chars.substr(first))
          chars.next
          escapeMap.get(chars.ch) match {
            case Some(s) => {
              sb.append(s)
              chars.next
            }
            case None => {
              if (chars.ch != 'u'.toInt) chars.error("Illegal escape")
              chars.next
              var code = 0
              for (i <- 1 to 4) {
                val ch = chars.ch.toChar.toString
                val i = "0123456789abcdef".indexOf(ch.toLowerCase)
                if (i == -1) chars.error("Illegal hex character")
                code = code * 16 + i
                chars.next
              }
              sb.append(code.toChar.toString)
            }
          }
          first = chars.mark
        } else {
          chars.next
        }
      }
      if (chars.ch != '"') chars.error("Unexpected string character:" + chars.ch.toChar)
      val s1 = chars.substr(first)
      val s2 = if (sb == null) s1 else {
        sb.append(s1)
        sb.toString
      }
      val result = (STRING, s2)
      chars.next
      result
    }

    private def handleLetter(chars: Chars) = {
      val first = chars.mark
      while (chars.kind == Letter) {
        chars.next
      }
      val s = chars.substr(first)
      (ID, s)
    }

    private def getDigits(chars: Chars) {
      while (chars.kind == Digit) {
        chars.next
      }
    }

    private def handleDigit(chars: Chars) = {
      val first = chars.mark
      getDigits(chars)
      val k1 = if (chars.ch == '.'.toInt) {
        chars.next
        getDigits(chars)
        BIGNUMBER
      } else {
        NUMBER
      }
      val k2 = if (chars.ch == 'E'.toInt || chars.ch == 'e'.toInt) {
        chars.next
        if (chars.ch == '+'.toInt) {
          chars.next
        } else if (chars.ch == '-'.toInt) {
          chars.next
        }
        BIGNUMBER
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
      } while (chars.kind == Blank)
      (BLANK, "")
    }

    private def handleUnexpected(chars: Chars) = {
      chars.error("Unexpected character")
    }

    private class Chars(val s: String) {
      private var pos = 0
      private val size = s.size
      var ch: Int = 0
      var kind: CharKind = Other
      var linePos: Int = 1
      var charPos = 0
      def next {
        if (pos < size) {
          ch = s(pos).toInt
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
      }
      def error(msg: String): Nothing = {
        throw new JsonParseException(msg, s, linePos, charPos)
      }
      def mark = pos - 1
      def substr(first: Int) = {
        s.substring(first, pos - 1)
      }
      next
    }

    class Tokens(val s: String) {
      private val chars = new Chars(s)
      var value: String = ""
      var kind: TokenKind = BLANK
      var linePos = 1
      var charPos = 1
      def next {
        do {
          linePos = chars.linePos
          charPos = chars.charPos
          val (k, v) = charAction(chars.kind.id)(chars)
          kind = k
          value = v
        } while (kind == BLANK)
      }
      def error(msg: String): Nothing = {
        throw new JsonParseException(msg, s, linePos, charPos)
      }
      next
    }

  }
  import lexer._
  import lexer.TokenKinds._

  private val tokenAction = new Array[(Tokens) => Json](TokenKinds.values.size)
  tokenAction(ID.id) = handleId
  tokenAction(STRING.id) = handleString
  tokenAction(NUMBER.id) = handleNumber
  tokenAction(BIGNUMBER.id) = handleBigNumber
  tokenAction(COLON.id) = handleUnexpected
  tokenAction(COMMA.id) = handleUnexpected
  tokenAction(LOBJ.id) = handleObject
  tokenAction(ROBJ.id) = handleUnexpected
  tokenAction(LARR.id) = handleArray
  tokenAction(RARR.id) = handleUnexpected
  tokenAction(EOF.id) = handleEof

  private def handleId(tokens: Tokens) = {
    val result = if (tokens.value == "true") {
      true
    } else if (tokens.value == "false") {
      false
    } else if (tokens.value == "null") {
      null
    } else {
      tokens.error("Not true, false, or null")
    }
    tokens.next
    result
  }

  private def handleString(tokens: Tokens) = {
    val result = tokens.value
    tokens.next
    result
  }

  private def handleNumber(tokens: Tokens) = {
    val v = try {
      tokens.value.toLong
    } catch {
      case _ => tokens.error("Bad integer")
    }
    tokens.next
    val r: Json = if (v >= Int.MinValue && v <= Int.MaxValue) v.toInt else v
    r
  }

  private def handleBigNumber(tokens: Tokens) = {
    val v = try {
      BigDecimal(tokens.value)
    } catch {
      case _ => tokens.error("Bad decimal number")
    }
    tokens.next
    v
  }

  private def handleArray(tokens: Tokens) = {
    tokens.next
    var result = List[Json]()
    while (tokens.kind != RARR) {
      val t = getJson(tokens)
      result = t +: result
      if (tokens.kind == COMMA) {
        tokens.next
      } else if (tokens.kind == RARR) {
      } else {
        tokens.error("Expecting , or ]")
      }
    }
    tokens.next
    result.reverse
  }

  private def handleObject(tokens: Tokens) = {
    tokens.next
    var result = new HashMap[String, Json]()
    while (tokens.kind != ROBJ) {
      if (tokens.kind != STRING) tokens.error("Expecting string")
      val name = tokens.value
      tokens.next
      if (tokens.kind != COLON) tokens.error("Expecting :")
      tokens.next
      val t = getJson(tokens)
      result += (name -> t)
      if (tokens.kind == COMMA) {
        tokens.next
      } else if (tokens.kind == ROBJ) {
      } else {
        tokens.error("Expecting , or }")
      }
    }
    tokens.next
    result
  }

  private def handleEof(tokens: Tokens) {
    tokens.error("Unexpected eof")
  }
  private def handleUnexpected(tokens: Tokens) {
    tokens.error("Unexpected input")
  }

  private def getJson(tokens: Tokens) = {
    tokenAction(tokens.kind.id)(tokens)
  }

  def parse(s: String): Json = {
    val tokens = new Tokens(s)
    val result = getJson(tokens)
    if (tokens.kind != EOF) tokens.error("Excess input")
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
        case x => throw new SystemException(Codes.JsonUnparse, JsonObject("msg" -> "bad json value", "value" -> x.toString()))
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

  private val WIDTH = 50
  private val COUNT = 6

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
        val strings = seq2.map { case (k, v) => {
          val v1 = pretty(v)
          val label = quote(k.toString) + ":"
          if (isMultiLine(v1) || label.size + v1.size > WIDTH) {
            label + "\n" + doIndent(v1,2)
          } else {
            label + v1
          }
        }}
        if (!split(strings)) {
          doIndent("{" + strings.mkString(",") + "}", indent)
        } else {
          wrap("{", ",", "}", indent, strings)
        }
      case s: String => doIndent(quote(s), indent)
      case x => {
        throw new SystemException(Codes.JsonUnparse, JsonObject("msg" -> "bad json value", "value" -> x.toString()))
      }
    }
  }

}