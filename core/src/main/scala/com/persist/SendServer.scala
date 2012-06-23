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

import akka.actor.ActorSystem

// Actor Paths
//       /user/@server
//       /user/database
//       /user/database/@send

private[persist] class SendServer(system:ActorSystem) {
  // TODO this should become an actor that sends a message to an actor
  // rather than returning the actor ref
  
  private var servers = Map[String,ServerRefs]()
  
  private case class DatabaseRefs(serverName:String, databaseName:String) {
    lazy val sendRef = system.actorFor("akka://ostore@" + serverName + "/user/" + databaseName + "/@send")
    lazy val databaseRef = system.actorFor("akka://ostore@" + serverName + "/user/" + databaseName)
  }
  
  private case class ServerRefs(serverName:String) {
    lazy val serverRef = system.actorFor("akka://ostore@" + serverName + "/user/" + "@server")
    private var databases = Map[String,DatabaseRefs]()
    def getDatabase(databaseName:String) = {
      databases.get(databaseName) match {
        case Some(refs) => refs
        case None => {
          val refs = DatabaseRefs(serverName, databaseName)
          databases += (databaseName -> refs)
          refs
        }
      }
      
    }
  }
  
  private def getServer(serverName:String) = {
    servers.get(serverName) match {
      case Some(refs) => refs
      case None => {
        val refs = ServerRefs(serverName)
        servers += (serverName -> refs)
        refs
      }
    }
  }
  
  def serverRef(serverName:String) = {
    getServer(serverName).serverRef
    
  }
  
  def sendRef(serverName:String,databaseName:String) = {
    getServer(serverName).getDatabase(databaseName).sendRef
  }
  
  def databaseRef(serverName:String,databaseName:String) = {
    getServer(serverName).getDatabase(databaseName).databaseRef
    
  }

}