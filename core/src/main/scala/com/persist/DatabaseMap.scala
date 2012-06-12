package com.persist
import akka.actor.ActorRef
import java.util.TreeMap
import scala.collection.mutable.HashMap
import akka.actor.ActorSystem

case class RingMap(var tables:HashMap[String,TableMap])

case class TableMap(
    var nodes:HashMap[String,NodeMap]) {
  // low -> nodeName
  var keys = new TreeMap[String,String]()
  // also array of NodeMap (by pos)
}

case class NodeMap(databaseName:String, ringName:String, nodeName:String, tableName:String,
    var pos:Int, host:String, port:String) {
  var known:Boolean = false
  var low:String = ""
  var high:String = ""
  private var ref:ActorRef = null
  def getRef(system:ActorSystem):ActorRef = {
    if (ref == null) {
       ref = system.actorFor("akka://ostore@" + host + ":" + port + "/user/" + 
            databaseName + "/" + ringName + "/" + nodeName + "/" + tableName)
    }
    ref
  }
}

object DatabaseMap {
  def apply(config:DatabaseConfig):DatabaseMap = {
    // TODO build structure
    val rings = HashMap[String,RingMap]()
    new DatabaseMap(config.name,rings)
  }
}
class DatabaseMap(databaseName:String,rings:HashMap[String,RingMap]) {
  
  def get(system:ActorSystem, ringName:String,nodeName:String,tableName:String,key:String,less:Boolean):ActorRef = {
    val ringMap = rings(ringName)
    val tableMap = ringMap.tables(tableName)
    // if only one node use it
    // TODO find in keys
    // if > high then use next (mark as in transit) 
    // if not found, pick unknown 
    null
  }
  
  def setLowHigh(ringName:String,nodeName:String,tableName:String,low:String,high:String) {
    val ringMap = rings(ringName)
    val tableMap = ringMap.tables(tableName)
    val nodeMap = tableMap.nodes(nodeName)
    if (! nodeMap.known) {
      // don't insert of low==high
      tableMap.keys.put(low,nodeName)
      // TODO more than 1 with same low???
    } else {
      // TODO remove and reinsert!
    }
    nodeMap.low = low
    nodeMap.high = high
    nodeMap.known = true
  }

}