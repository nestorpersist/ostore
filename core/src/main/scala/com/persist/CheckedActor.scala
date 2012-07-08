package com.persist


import akka.actor.Actor

private[persist] abstract class CheckedActor extends Actor with akka.actor.ActorLogging {
  def rec: PartialFunction[Any, Unit]
  def receive: PartialFunction[Any, Unit] = {
    case msg => {
      try {
        val body1: PartialFunction[Any, Unit] = rec.orElse {
          case x: Any => { log.error("Unmatched message " + x.toString() + " : " + self.toString()) }
        }
        body1(msg)
      } catch {
        case ex: Exception => {
          log.error(ex, "Unhandled exception in %s while processing %s".format(self.toString(), msg.toString()))
        }
      }
    }
  }
}
