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
import scala.util.parsing.combinator._
import scala.collection.immutable.HashSet

private[persist] object Text {
  private val stemmer = new Stemmer

  private def fix(w: String): Option[String] = {
    val s0 = w.toLowerCase()
    stemmer.add(s0)
    val s = if (stemmer.b.length > 2) {
      stemmer.step1()
      stemmer.step2()
      stemmer.step3()
      stemmer.step4()
      stemmer.step5a()
      stemmer.step5b()
      stemmer.b
    } else {
      s0
    }
    if (stop.contains(s)) {
      None
    } else {
      Some(s)
    }
  }

  class Words(kind: String, title: String) extends RegexParsers {
    private val word = """[a-zA-Z]+""".r
    private val ignore = """[^a-zA-Z]+""".r
    def combine(w: Option[String], a: List[(JsonKey, Json)]): List[(JsonKey, Json)] = {
      w match {
        case None => a
        case Some(s: String) => (JsonArray(kind, s, title), null) :: a
      }
    }
    override val whiteSpace = ignore
    def all: Parser[List[(JsonKey, Json)]] = (
      w ~ all ^^ { case w ~ a => combine(w, a) }
      | w ^^ { case w => combine(w, Nil) })
    def w: Parser[Option[String]] = word ^^ { case w => fix(w) }
  }

  private class Query extends RegexParsers {
    private val word = """[a-zA-Z:]+""".r
    private val ignore = """[^a-zA-Z:]+""".r
    def combine(w: Option[String], a: List[String]): List[String] = {
      w match {
        case None => a
        case Some(s: String) => s :: a
      }
    }
    override val whiteSpace = ignore
    def all: Parser[List[String]] = (
      w ~ all ^^ { case w ~ a => combine(w, a) }
      | w ^^ { case w => combine(w, Nil) })
    def w: Parser[Option[String]] = word ^^ { case w => fix(w) }
  }

  /*
  class MapTextIndex(var options: Json) extends MapReduce.Map {
    def to(key: JsonKey, value: Json): Traversable[(JsonKey, Json)] = {
      val title = jgetString(key)
      val words = jgetString(value)
      val s1 = {
        val p1 = new Words("", title)
        p1.parseAll(p1.all, words).get
      }
      val s2 = {
        val p2 = new Words("title", title)
        p2.parseAll(p2.all, title).get
      }
      s1 ::: s2
    }
    def from(key: JsonKey): JsonKey = jgetArray(key, 1)
  }
  */

  class TextSearch(c: Table) {

    private def next(kind: String, word: String, title: String, include: Boolean): String = {
      for (key <- c.all(JsonObject("low" -> JsonArray(kind, word, title), "includelow" -> include))) {
        if (jgetString(key, 0) != kind || jgetString(key, 1) != word) return ""
        return jgetString(key, 2)
      }
      ""
    }

    def find(query: String): Json = {
      val p1 = new Query
      val words = p1.parseAll(p1.all, query).get
      val size = words.size
      case class Winfo(val kind: String, val word: String)
      var infos = new Array[Winfo](size)
      var titles = new Array[String](size)
      var i = 0
      for (word <- words) {
        val parts = word.split(":")
        val info = if (parts.size == 2) {
          Winfo(parts(0), parts(1))
        } else {
          Winfo("", parts(0))
        }
        infos(i) = info
        titles(i) = ""
        i += 1
      }
      val word = words.head
      var result = JsonArray()
      var title = ""
      var cnt = 0
      while (cnt < 10) {
        var min:String = titles(0)
        var max:String = min
        for (pos <- 1 until size) {
          val title = titles(pos)
          if (title < min) min = title
          if (title > max) max = title
        }
        if (min == max) {
          if (min != "") result = min +: result
          for (pos <- 0 until size) {
            val info = infos(pos)
            val title = next(info.kind, info.word, min, false)
            if (title == "") return result.reverse
            titles(pos) = title
          }
        } else {
          for (pos <- 0 until size) {
            if (titles(pos) < max) {
              val info = infos(pos)
              val title = next(info.kind, info.word, max, true)
              if (title == "") return result.reverse
              titles(pos) = title
            }
          }
        }
        cnt += 1
      }
      result.reverse
    }
  }

  private val stop = HashSet[String]() ++: List(
    "a", "able", "about", "across", "after", "all", "almost", "also", "am", "among",
    "an", "and", "any", "are", "as", "at", "be", "because", "been", "but", "by",
    "can", "cannot", "could", "dear", "did", "do", "does", "either", "else", "ever", "every",
    "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers", "him", "his", "how",
    "however", "i", "if", "in", "into", "is", "it", "its", "just",
    "least", "let", "like", "likely", "may", "me", "might", "most", "must", "my",
    "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own",
    "rather", "said", "say", "says", "she", "should", "since", "so", "some",
    "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas",
    "us", "wants", "was", "we", "were", "what", "when", "where", "which", "while", "who",
    "whom", "why", "will", "with", "would", "yet", "you", "your")

}
