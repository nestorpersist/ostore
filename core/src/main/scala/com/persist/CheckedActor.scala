package com.persist


import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern._
import akka.util.duration._
import akka.dispatch.Await

private[persist] object Log {
  def log(msg: String) = println(msg)
  def log(msg: String,ex:Exception) = {
   println(msg)
   ex.printStackTrace()
  }
}

private[persist] abstract class CheckedActor extends Actor {
  def rec: PartialFunction[Any, Unit]
  def receive: PartialFunction[Any, Unit] = {
    case msg => {
      try {
        val body1: PartialFunction[Any, Unit] = rec.orElse {
          case x: Any => { Log.log("Unmatched message " + x.toString() + " : " + self.toString()) } 
        }
        body1(msg)
      } catch {
        case ex: Exception => {
          Log.log("Unhandled exception " + msg.toString() + " : " + ex.toString() + " : " + self.toString(),ex)
        }
      }
    }
  }
}
