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
import JsonKeys._

private[persist] trait ActComponent { this: UIAssembly =>
  val act: UIAct

  class UIAct(client: WebClient, page: Page) {
    var databaseName = ""

    private var databaseStatus = ""

    // At least one of lowKey highKey should always be null
    var tableName = ""
    var tableReadOnly = false
    private var hasBefore = false
    //private var lowKey: Json = null
    private var firstKey: Option[JsonKey] = None
    private var hasAfter = false
    //private var highKey: Json = null
    private var lastKey: Option[JsonKey] = None
    // TODO make itemCount depend on window size
    val itemCount = 20

    var key: JsonKey = null
    var cv: Json = null
    var ringName: String = ""
    private var nodeName: String = ""
    var itemVal: Json = null
    private var prefixVal = JsonArray()
    private val act = this

    def vcToString(vc: Json): String = {
      var result = ""
      for (ringName <- jgetObject(vc).keys) {
        val t = jgetLong(vc, ringName)
        // ISO 8601
        val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
        val d = format.format(new java.util.Date(t))
        result += (ringName + ": " + d + "\n")
      }
      result
    }
    def setHome() {
      buttons.databaseButton.setVisible(false)
      buttons.tableButton.setVisible(false)
      buttons.ringButton.setVisible(false)
      right.setName("")
      right.setMode("")
    }

    private def setDatabase(databaseName: String) {
      this.databaseName = databaseName
      buttons.databaseButton.setCaption("Database: " + databaseName)
      setHome()
      buttons.databaseButton.setVisible(true)
    }

    private def setTable(databaseName: String, tableName: String) {
      this.tableName = tableName
      buttons.tableButton.setCaption("Table: " + tableName)
      setDatabase(databaseName)
      buttons.tableButton.setVisible(true)
    }

    private def setRing(databaseName: String, ringName: String) {
      this.ringName = ringName
      buttons.ringButton.setCaption("Ring: " + ringName)
      setDatabase(databaseName)
      buttons.ringButton.setVisible(true)
    }

    def toDatabases() {
      val databases = client.getDatabases()
      setHome()
      left.setName("Database", true)
      left.setAct((databaseName: String) => {
        if (databaseName != null) {
          toDatabase(databaseName)
        }
      },
        _ => {
          FileWindows.createDatabase(all.all, client, () => toDatabases())
        })
      left.clear()
      for (database <- jgetArray(databases)) {
        left.add(jgetString(database))
      }
    }

    def toTables(databaseName: String) {
      val tables = client.getTables(databaseName)
      setDatabase(databaseName)
      left.setName("Table", true)
      left.setAct((tableName: String) => {
        if (tableName != null) {
          toTable(databaseName, tableName)
        }
      },
        _ => {
          editWindow.addTable(all.all, databaseName, client)
        })
      left.clear()
      for (table <- jgetArray(tables)) {
        left.add(jgetString(table))
      }
    }

    def toRings(databaseName: String) {
      val rings = client.getRings(databaseName)
      setDatabase(databaseName)
      left.setName("Ring", true)
      left.setAct((ringName: String) => {
        if (ringName != null) {
          toRing(databaseName, ringName)
        }
      },
        _ => {
          FileWindows.change(true, databaseName, "Ring", all.all, client, () => toRings(databaseName))
        })
      left.clear()
      for (ring <- jgetArray(rings)) {
        left.add(jgetString(ring))
      }
    }

    def toServers(databaseName: String) {
      val servers = client.getServers(databaseName)
      setDatabase(databaseName)
      left.setName("Server", false)
      left.setAct((serverName: String) => {
        if (serverName != null) {
          toServer(databaseName, serverName)
        }
      }, _ => {})
      left.clear()
      for (server <- jgetArray(servers)) {
        left.add(jgetString(server))
      }
    }

    def toKeys(databaseName: String, tableName: String, where: String) {
      val (hasUp, keys, hasDown) = if (where == "up") {
        page.up(databaseName, tableName, firstKey, itemCount, None)
      } else if (where == "down") {
        page.down(databaseName, tableName, lastKey, itemCount, None)
      } else {
        page.down(databaseName, tableName, None, itemCount, None)
      }
      firstKey = keys.headOption
      lastKey = keys.lastOption
      setTable(databaseName, tableName)
      left.setName("Item", ! tableReadOnly)
      left.setUpDown(
        if (hasUp) { _ => { toKeys(databaseName, tableName, "up") } } else { null },
        if (hasDown) { _ => { toKeys(databaseName, tableName, "down") } } else { null })
      left.setAct((key: String) => {
        if (key != null) {
          toItem(databaseName, tableName, Json(key))
        }
      },
        _ => {
          editWindow.add(all.all, databaseName, tableName, client)
        })
      left.clear()
      for (key <- jgetArray(keys)) {
        left.add(Compact(key))
      }
    }

    def toTree(databaseName: String, tableName: String, prefix: JsonArray, where: String) {
      val prefixs = Compact(prefix)
      val (hasUp, keys, hasDown) = if (where == "up") {
        page.up(databaseName, tableName, firstKey, itemCount, Some(prefix))
      } else if (where == "down") {
        page.down(databaseName, tableName, lastKey, itemCount, Some(prefix))
      } else {
        page.down(databaseName, tableName, None, itemCount, Some(prefix))
      }
      firstKey = keys.headOption
      lastKey = keys.lastOption
      setTable(databaseName, tableName)
      left.setName("Tree:" + prefixs, false)
      left.setUpDown(
        if (hasUp) { _ => { toTree(databaseName, tableName, prefix, "up") } } else { null },
        if (hasDown) { _ => { toTree(databaseName, tableName, prefix, "down") } } else { null })
      left.setAct((key: String) => {
        if (key != null) {
          if (key == prefixs) {
            // Current
            toItem(databaseName, tableName, prefix)
          } else if (key == "..") {
            // Parent
            if (prefix.size > 0) {
              val prefix1 = prefix.dropRight(1)
              toTree(databaseName, tableName, prefix1, "")
            }
          } else {
            // Child
            val prefix1 = jgetArray(Json(key))
            val (hasUp1, keys1, hasDown1) = page.down(databaseName, tableName, None, 1, Some(prefix1))
            if (jsize(keys1) > 0) {
              toTree(databaseName, tableName, prefix1, "")
            } else {
              toItem(databaseName, tableName, Json(key))
            }
          }
        }
      },
        _ => { // No add button
        })
      left.clear()
      var first = true
      for (key <- jgetArray(keys)) {
        if (first) {
          if (prefix.size > 0) left.add("..")
          if (!hasUp) left.add(prefixs)
        }
        left.add(Compact(key))
        first = false
      }
    }

    def toNodes(databaseName: String, ringName: String) {
      val nodes = client.getNodes(databaseName, ringName)
      setRing(databaseName, ringName)
      left.setName("Node", true)
      left.setAct((nodeName: String) => {
        if (nodeName != null) {
          toNode(databaseName, ringName, nodeName)
        }
      },
        _ => {
          FileWindows.change(true, databaseName, "Node", all.all, client, () => toNodes(databaseName,ringName))
        })
      left.clear()
      for (node <- jgetArray(nodes)) {
        left.add(jgetString(node))
      }
    }

    def toDatabase(databaseName: String) {
      this.databaseName = databaseName
      val status = client.getDatabaseStatus(databaseName)
      this.databaseStatus = status
      right.setStatus(status)
      buttons.dbTables.setVisible(status == "active")
      buttons.dbRings.setVisible(status == "active")
      buttons.dbServers.setVisible(status == "active")
      buttons.dbStop.setVisible(status == "active")
      buttons.dbStart.setVisible(status == "stop")
      buttons.dbDelete.setVisible(status == "stop")
      right.setName("Database: " + databaseName)
      right.setMode("database")
    }

    def toTable(databaseName: String, tableName: String) {
      val info = client.getTableInfo(databaseName, tableName)
      val ro = jgetBoolean(info, "r")
      val t = jgetObject(info, "t")
      buttons.tableUpload.setVisible(! ro)
      this.databaseName = databaseName
      this.tableName = tableName
      this.tableReadOnly = ro
      right.setMode("table")
      right.setReadOnly(ro, t)
      right.setName("Table: " + tableName)
    }

    def toRing(databaseName: String, ringName: String) {
      this.databaseName = databaseName
      this.ringName = ringName
      right.setName("Ring: " + ringName)
      right.setMode("ring")
    }

    def toServer(databaseName: String, serverName: String) {
      this.databaseName = databaseName
      this.tableName = tableName
      right.setName("Server: " + serverName)
      right.setMode("server")
    }

    def toItem(databaseName: String, tableName: String, key: JsonKey) {
      val item = try {
        client.getItem(databaseName, tableName, key)
      } catch {
        case ex: Exception => {
          right.setName("")
          right.setMode("")
          return
        }
      }
      val v = jget(item, "v")
      val cv = jget(item, "c")
      this.databaseName = databaseName
      this.tableName = tableName
      this.key = key
      this.cv = cv
      right.setName("Item: " + Compact(key))
      right.setMode("item")
      right.ta.setReadOnly(false)
      itemVal = v
      val p = Pretty(v)
      right.ta.setValue(p)
      right.ta.setRows(right.rows(p))
      right.ta.setReadOnly(true)

      right.vta.setReadOnly(false)
      val vcs = vcToString(cv)
      right.vta.setValue(vcs)
      right.vta.setRows(right.rows(vcs))
      right.vta.setReadOnly(true)
      
      buttons.editItem.setVisible(! tableReadOnly)
      buttons.deleteItem.setVisible(! tableReadOnly)
    }

    def toNode(databaseName: String, ringName: String, nodeName: String) {
      this.databaseName = databaseName
      this.ringName = ringName
      this.nodeName = nodeName
      right.setName("Node: " + nodeName)
      right.setMode("node")
    }

  }
}
