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

import akka.actor._
import akka.util.{ ByteString, ByteStringBuilder }
import java.net.InetSocketAddress
import JsonOps._
import akka.actor.IO.ServerHandle
import akka.dispatch.Await
import akka.pattern._
import akka.util.duration._
import akka.util.Timeout
import akka.dispatch.Future
import Exceptions._

private[persist] object HttpConstants {
  val SP = ByteString(" ")
  val HT = ByteString("\t")
  val CRLF = ByteString("\r\n")
  val COLON = ByteString(":")
  val PERCENT = ByteString("%")
  val PATH = ByteString("/")
  val QUERY = ByteString("?")
}

private[persist] case class Request(meth: String, path: List[String], query: Option[String], httpver: String, headers: List[Header], body: Option[ByteString])
private[persist] case class Header(name: String, value: String)

private[persist] object HttpIteratees {
  import HttpConstants._

  def readRequest =
    for {
      requestLine <- readRequestLine
      (meth, (path, query), httpver) = requestLine
      headers <- readHeaders
      body <- readBody(headers)
    } yield Request(meth, path, query, httpver, headers, body)

  def ascii(bytes: ByteString): String = bytes.decodeString("US-ASCII").trim

  def readRequestLine =
    for {
      meth <- IO takeUntil SP
      uri <- readRequestURI
      _ <- IO takeUntil SP // ignore the rest
      httpver <- IO takeUntil CRLF
    } yield (ascii(meth), uri, ascii(httpver))

  def readRequestURI = IO peek 1 flatMap {
    case PATH =>
      for {
        path <- readPath
        query <- readQuery
      } yield (path, query)
    case _ => sys.error("Not Implemented")
  }

  def readPath = {
    def step(segments: List[String]): IO.Iteratee[List[String]] = IO peek 1 flatMap {
      case PATH => IO drop 1 flatMap (_ => readUriPart(pathchar) flatMap (segment => step(segment :: segments)))
      case _ => segments match {
        case "" :: rest => IO Done rest.reverse
        case _ => IO Done segments.reverse
      }
    }
    step(Nil)
  }

  def readQuery: IO.Iteratee[Option[String]] = IO peek 1 flatMap {
    case QUERY => IO drop 1 flatMap (_ => readUriPart(querychar) map (Some(_)))
    case _ => IO Done None
  }

  val alpha = Set.empty ++ ('a' to 'z') ++ ('A' to 'Z') map (_.toByte)
  val digit = Set.empty ++ ('0' to '9') map (_.toByte)
  val hexdigit = digit ++ (Set.empty ++ ('a' to 'f') ++ ('A' to 'F') map (_.toByte))
  val subdelim = Set('!', '$', '&', '\'', '(', ')', '*', '+', ',', ';', '=') map (_.toByte)
  //val pathchar = alpha ++ digit ++ subdelim ++ (Set(':', '@') map (_.toByte))
  val pathchar = alpha ++ digit ++ subdelim ++ (Set(':', '@', '.') map (_.toByte))
  val querychar = pathchar ++ (Set('/', '?') map (_.toByte))

  def readUriPart(allowed: Set[Byte]): IO.Iteratee[String] = for {
    str <- IO takeWhile allowed map ascii
    pchar <- IO peek 1 map (_ == PERCENT)
    all <- if (pchar) readPChar flatMap (ch => readUriPart(allowed) map (str + ch + _)) else IO Done str
  } yield all

  def readPChar = IO take 3 map {
    case Seq('%', rest @ _*) if rest forall hexdigit =>
      java.lang.Integer.parseInt(rest map (_.toChar) mkString, 16).toChar
  }

  def readHeaders = {
    def step(found: List[Header]): IO.Iteratee[List[Header]] = {
      IO peek 2 flatMap {
        case CRLF => IO takeUntil CRLF flatMap (_ => IO Done found)
        case _ => readHeader flatMap (header => step(header :: found))
      }
    }
    step(Nil)
  }

  def readHeader =
    for {
      name <- IO takeUntil COLON
      value <- IO takeUntil CRLF flatMap readMultiLineValue
    } yield Header(ascii(name), ascii(value))

  def readMultiLineValue(initial: ByteString): IO.Iteratee[ByteString] = IO peek 1 flatMap {
    case SP => IO takeUntil CRLF flatMap (bytes => readMultiLineValue(initial ++ bytes))
    case _ => IO Done initial
  }

  def readBody(headers: List[Header]) =
    headers.find(header => header.name == "Content-Length") match {
      case Some(h: Header) => {
        val n = h.value.toInt
        val r = IO take n map { Some(_) }
        IO Done None
        r
      }
      case x => {
        if (headers.exists(header => header.name == "Transfer-Encoding")) {
          IO.takeAll map (Some(_))
        } else {
          IO Done None
        }
      }
    }
}

private[persist] class HttpServer(port: Int) extends Actor {

  val state = IO.IterateeRef.Map.async[IO.Handle]()(context.dispatcher)
  var channel: ServerHandle = null // .close() to shutdown

  override def preStart {
    channel = IOManager(context.system) listen new InetSocketAddress(port)
  }

  def receive = {
    case ("start") => sender ! Codes.Ok
    case ("stop") => {
      channel.close()
      sender ! Codes.Ok
    }
    case IO.NewClient(server) =>
      val socket = server.accept()
      state(socket) flatMap (_ => HttpServer.processRequest(socket))
    case IO.Read(socket, bytes) =>
      state(socket)(IO Chunk bytes)
    case IO.Closed(socket, cause) =>
      state(socket)(IO EOF None)
      state -= socket
  }
}

private[persist] object HttpServer {
  import HttpIteratees._

  // TODO must add headers to response
  case class OKResponse(val bytes: ByteString, val keepAlive: Boolean = false)

  case class Response(val code: Int, val cause: String, val result: String)

  def doRequest(method: String, path: String, q: String, body: String): Future[Response] = {
    val jbody = if (method == "put" || method == "post") {
      if (body == "") throw RequestException("missing body for " + method)
      try {
        Json(body)
      } catch {
        case ex: Exception => throw RequestException("Can't parse json input: " + ex.getMessage())
      }
    } else {
      null
    }
    val (isPretty, f): (Boolean, Future[Option[Json]]) = RestClient.doAll(method, path, q, jbody)
    val f1 = f.map { result =>
      result match {
        case Some(j) => {
          Response(200, "OK", if (isPretty) { Pretty(j) } else { Compact(j) })
        }
        case None => {
          Response(200, "OK", "")
        }
      }
    }
    val f2 = f1.recover { r =>
      r match {
        //case ex: BadRequestException => Response(400, "BAD_REQUEST", ex.msg)
        //case ex: ConflictException => Response(409, "CONFLICT", ex.msg)
        case ex:Exception => {
          val (httpCode, short, msg) = exceptionToHttp(ex) 
          Response(httpCode, short, Compact(msg))
        }
        case x => {
          println("RECOVER FAILURE:"+x)
          Response(500, "recover", "bad")
        }
      }
    }
    f2
  }

  def processRequest(socket: IO.SocketHandle): IO.Iteratee[Unit] =
    IO repeat {
      for {
        request <- readRequest
      } yield {
        val method = request.meth.toLowerCase()
        var path = ""
        var first = true
        for (p <- request.path) {
          if (!first) path += "/"
          path += p
          first = false
        }
        val q = request.query.getOrElse("")
        val body = request.body match {
          case Some(b: ByteString) => b.decodeString("UTF-8")
          case _ => ""
        }
        val keepAlive = request.headers.exists { case Header(n, v) => n.toLowerCase == "connection" && v.toLowerCase == "keep-alive" }
        val responsef = doRequest(method, path, q, body)
        responsef map { response =>
          val result1 = if (response.result == "") {
            ""
          } else {
            (if (response.code == 200) { "Content-Type: application/json\n" } else { "" }) +
              "Content-Length: " + response.result.length() + "\n" +
              "\n" +
              response.result
          }
          val r = "HTTP/1.0 " + response.code + " " + response.cause + "\n" + result1
          socket write ByteString(r).compact
          if (keepAlive) socket.close()
        }
      }
    }
}

private[persist] object Http {
  var server: ActorRef = null
  var system: ActorSystem = null
  implicit val timeout = Timeout(5 seconds)

  def start(system: ActorSystem, port: Int) {
    this.system = system
    server = system.actorOf(Props(new HttpServer(port)))
    val f = server ? ("start")
    Await.result(f, 5 seconds)
    println("Running rest server on port " + port)
  }

  def stop {
    val f = server ? ("stop")
    Await.result(f, 5 seconds)
    system.stop(server)
  }

}
