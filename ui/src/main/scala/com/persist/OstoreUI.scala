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

import com.vaadin.Application
import com.vaadin.ui._
import com.vaadin.ui.Button.ClickListener
import com.vaadin.terminal.Sizeable
import com.vaadin.ui.themes._
import JsonUtil._
import scala.collection.JavaConversions._
import com.vaadin.data.Property
import java.io.File
import com.vaadin.terminal.FileResource
import com.vaadin.terminal.StreamResource
import java.io.InputStream
import java.io.ByteArrayInputStream

// TODO change icon

class StringResource(s: String) extends StreamResource.StreamSource {
  def getStream(): InputStream = {
    new ByteArrayInputStream(s.getBytes("UTF-8"))
  }
}

class Top {
  val all = new HorizontalLayout()
}

class Left {
  private var selectAct: String => Unit = { (s: String) => { println(s) } }
  private var addAct: Unit => Unit = { _ => println("ADD***") }
  private var upAct: Unit => Unit = null
  private var downAct : Unit => Unit = null
  val all = new Panel("")
  all.setSizeFull()
  all.getContent().setSizeFull()
  private val l1 = new VerticalLayout()
  l1.setSizeFull()
  all.addComponent(l1)
  private val add = new Button("")
  l1.addComponent(add)
  private val up = new Button("Up")
  l1.addComponent(up)
  val list = new ListSelect()
  list.setSizeFull()
  list.setImmediate(true)
  l1.addComponent(list)
  l1.setExpandRatio(list, 1.0F)
  private val down = new Button("Down")
  l1.addComponent(down)

  list.addListener(new Property.ValueChangeListener() {
    def valueChange(event: Property.ValueChangeEvent) {
      val name: String = list.getValue().asInstanceOf[String]
      selectAct(name)
    }
  })

  add.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      addAct()
    }
  })

  up.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      upAct()
    }
  })

  down.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      downAct()
    }
  })

  def clear() { list.removeAllItems() }

  def add(s: String) { list.addItem(s) }

  def setName(name: String) {
    if (name.contains(":")) {
      all.setCaption(name)
      add.setVisible(false)
    } else {
      all.setCaption(name + "s")
      add.setCaption("Add " + name)
      add.setVisible(true)
    }
    up.setVisible(name == "Item" || name.startsWith("Tree:"))
    down.setVisible(name == "Item" || name.startsWith("Tree:"))
  }
  
  def setUpDown(upAct:Unit => Unit, downAct: Unit => Unit) {
    up.setEnabled(upAct != null)
    down.setEnabled(downAct != null)
    this.upAct = upAct
    this.downAct = downAct
  }

  def setAct(selectAct: String => Unit, addAct: Unit => Unit) {
    this.selectAct = selectAct
    this.addAct = addAct
  }

}

class Right {
  val all = new Panel("")
  all.setSizeFull()
  all.getContent().setSizeFull()
  private val r1 = new VerticalLayout()
  r1.setSizeFull()
  all.addComponent(r1)
  val databaseButtons = new CssLayout()
  r1.addComponent(databaseButtons)
  val tableButtons = new HorizontalLayout()
  r1.addComponent(tableButtons)
  val itemButtons = new HorizontalLayout()
  r1.addComponent(itemButtons)
  val ringButtons = new HorizontalLayout()
  r1.addComponent(ringButtons)
  val nodeButtons = new HorizontalLayout()
  r1.addComponent(nodeButtons)

  val status = new Label("")
  r1.addComponent(status)

  val ta = new TextArea("Value")
  ta.setSizeFull()
  r1.addComponent(ta)
  r1.setExpandRatio(ta, 1.0F)
  ta.setVisible(false)

  val vta = new TextArea("Vector Clock")
  vta.setSizeFull()
  r1.addComponent(vta)
  r1.setExpandRatio(vta, 0.3f)
  vta.setVisible(false)

  def setName(name: String) {
    all.setCaption(name)
  }
  def setStatus(stat: String) {
    status.setValue("Status: " + stat)
  }
  def setMode(mode: String) {
    databaseButtons.setVisible(mode == "database")
    status.setVisible(mode == "database")
    tableButtons.setVisible(mode == "table")
    itemButtons.setVisible(mode == "item")
    ringButtons.setVisible(mode == "ring")
    nodeButtons.setVisible(mode == "node")
    ta.setVisible(mode == "item")
    vta.setVisible(mode == "item")
  }
}

class All(top: Component, left: Component, right: Component) {
  // Overall Panel Structure
  val all = new Window("OStore Manager")
  all.getContent().setSizeFull()

  private val v = new VerticalLayout()
  v.setSizeFull()
  all.addComponent(v)

  private val h = new HorizontalSplitPanel()
  h.setSizeFull()

  v.addComponent(top)
  v.addComponent(h)
  v.setExpandRatio(h, 1.0F)

  h.addComponent(left)
  h.addComponent(right)
}

class Act(app: Application, client: WebClient, all: All, top: Top, left: Left, right: Right) {
  private var databaseName = ""
  private var databaseStatus = ""
 
  // At least one of lowKey highKey should always be null
  private var tableName = ""
  private var hasBefore = false
  private var lowKey:Json = null
  private var hasAfter = false
  private var highKey:Json = null
  private val itemCount = 20
  
  private var key: JsonKey = null
  private var cv: Json = null
  private var ringName: String = ""
  private var nodeName: String = ""
  private var itemVal: Json = null
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

  // Top Buttons
  private val homeButton = new Button("Home")
  homeButton.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toDatabases()
    }
  })
  private val databaseButton = new Button("")
  databaseButton.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toDatabases()
      left.list.select(databaseName)
      toDatabase(databaseName)
    }
  })
  private val tableButton = new Button("")
  tableButton.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toTables(databaseName)
      left.list.select(tableName)
      toTable(databaseName, tableName)
    }
  })
  private val ringButton = new Button("")
  ringButton.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toRings(databaseName)
      left.list.select(ringName)
      toRing(databaseName, ringName)
    }
  })
  top.all.addComponent(homeButton)
  top.all.addComponent(databaseButton)
  top.all.addComponent(tableButton)
  top.all.addComponent(ringButton)

  // Right Database Buttons
  val dbTables = new Button("Show Tables")
  dbTables.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toTables(databaseName)
    }
  })
  val dbRings = new Button("Show Rings")
  dbRings.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toRings(databaseName)
    }
  })
  val dbServers = new Button("Show Servers")
  dbServers.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toServers(databaseName)
    }
  })
  val dbStop = new Button("Stop Database")
  dbStop.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      client.stopDatabase(databaseName)
      toDatabase(databaseName)
    }
  })
  val dbStart = new Button("Start Database")
  dbStart.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      client.startDatabase(databaseName)
      toDatabase(databaseName)
    }
  })
  val dbDelete = new Button("Delete Database")
  dbDelete.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      client.deleteDatabase(databaseName)
      toDatabases()
    }
  })
  right.databaseButtons.addComponent(dbTables)
  right.databaseButtons.addComponent(dbRings)
  right.databaseButtons.addComponent(dbServers)
  right.databaseButtons.addComponent(dbStop)
  right.databaseButtons.addComponent(dbStart)
  right.databaseButtons.addComponent(dbDelete)

  // Right Table Buttons
  val t1 = new Button("Show Items")
  t1.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toKeys(databaseName, tableName, true)
    }
  })
  val t2 = new Button("Show Tree")
  t2.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toTree(databaseName, tableName, JsonArray(),true)
    }
  })
  val t3 = new Button("Delete Table")
  t3.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
    }
  })
  val t4 = new Button("Upload")
  t4.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      FileWindows.load(all.all, databaseName, tableName, client)
    }
  })
  val t5 = new Button("Download")
  t5.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      // TODO should stream in for large tables
      // TODO deal with paging for large tables
      val items = client.getItems(databaseName, tableName,itemCount + 1)
      var result = ""
      for (item <- jgetArray(items)) {
        result += Compact(jget(item, "k")) + "\t" + Compact(jget(item, "v")) + "\n"
      }
      val sr = new StreamResource(new StringResource(result), databaseName + "." + tableName + ".json", app)
      all.all.open(sr)
    }
  })
  right.tableButtons.addComponent(t1)
  right.tableButtons.addComponent(t2)
  right.tableButtons.addComponent(t3)
  right.tableButtons.addComponent(t4)
  right.tableButtons.addComponent(t5)

  // Right Item Buttons
  val i1 = new Button("Delete Item")
  i1.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      def finish(delete:Boolean) = {
        if (delete) {
          client.deleteItem(databaseName, tableName, key)
        }
        toKeys(databaseName, tableName, false)
      }
      val ok = client.conditionalDeleteItem(databaseName, tableName, key, cv)
      if (ok) {
        finish(false)
      } else {
        EditWindow.test(finish,all.all, "Conflict Detected", "Item has changed","Delete","Cancel")
      }
    }
  })
  val i2 = new Button("Edit Item")
  i2.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      EditWindow.edit(act, all.all, databaseName, tableName, key, cv, itemVal, client)
    }
  })
  right.itemButtons.addComponent(i1)
  right.itemButtons.addComponent(i2)

  // Right Ring Buttons
  val r1 = new Button("Show Nodes")
  r1.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
      toNodes(databaseName, ringName)
    }
  })
  val r2 = new Button("Delete Ring")
  r2.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
    }
  })
  right.ringButtons.addComponent(r1)
  right.ringButtons.addComponent(r2)

  // Right Node Buttons
  val n1 = new Button("Delete Node")
  n1.addListener(new ClickListener {
    def buttonClick(e: Button#ClickEvent) = {
    }
  })
  right.nodeButtons.addComponent(n1)

  private def setHome() {
    databaseButton.setVisible(false)
    tableButton.setVisible(false)
    ringButton.setVisible(false)
    right.setName("")
    right.setMode("")
  }

  private def setDatabase(databaseName: String) {
    this.databaseName = databaseName
    databaseButton.setCaption("Database: " + databaseName)
    setHome()
    databaseButton.setVisible(true)
  }

  private def setTable(databaseName: String, tableName: String) {
    this.tableName = tableName
    tableButton.setCaption("Table: " + tableName)
    setDatabase(databaseName)
    tableButton.setVisible(true)
  }

  private def setRing(databaseName: String, ringName: String) {
    this.ringName = ringName
    ringButton.setCaption("Ring: " + ringName)
    setDatabase(databaseName)
    ringButton.setVisible(true)
  }

  def toDatabases() {
    val databases = client.getDatabases()
    setHome()
    left.setName("Database")
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
    left.setName("Table")
    left.setAct((tableName: String) => {
      if (tableName != null) {
        toTable(databaseName, tableName)
      }
    },
      _ => {})
    left.clear()
    for (table <- jgetArray(tables)) {
      left.add(jgetString(table))
    }
  }

  def toRings(databaseName: String) {
    val rings = client.getRings(databaseName)
    setDatabase(databaseName)
    left.setName("Ring")
    left.setAct((ringName: String) => {
      if (ringName != null) {
        toRing(databaseName, ringName)
      }
    },
      _ => {})
    left.clear()
    for (ring <- jgetArray(rings)) {
      left.add(jgetString(ring))
    }
  }

  def toServers(databaseName: String) {
    val servers = client.getServers(databaseName)
    setDatabase(databaseName)
    left.setName("Server")
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

  def toKeys(databaseName: String, tableName: String, reset:Boolean) {
    if (reset) {
      lowKey = null
      highKey = null
    }
    val (hasMore, keys) = client.getKeys(databaseName, tableName, itemCount, lowKey, highKey)
    if (highKey != null) {
      hasBefore = hasMore
      hasAfter = true
    } else if (lowKey != null) {
      hasBefore = true
      hasAfter = hasMore
    } else {
      hasBefore = false
      hasAfter = hasMore
    }
    setTable(databaseName, tableName)
    left.setName("Item")
    left.setUpDown(
        if (hasBefore) { _ => {highKey = lowKey; lowKey = null; toKeys(databaseName, tableName, false)} } else { null },
        if (hasAfter) { _ => {lowKey = highKey; highKey = null; toKeys(databaseName, tableName, false)} } else { null }
          )
    left.setAct((key: String) => {
      if (key != null) {
        toItem(databaseName, tableName, Json(key))
      }
    },
      _ => {
        EditWindow.add(act, all.all, databaseName, tableName, client)
      })
    left.clear()
    lowKey = null
    highKey = null
    var first = true
    for (key <- jgetArray(keys)) {
      if (first) lowKey = key
      highKey = key
      left.add(Compact(key)) 
      first = false
    }
  }

  def toTree1(databaseName: String, tableName: String, prefix: JsonArray, reset:Boolean, hasMore:Boolean, keys:Json) {
    val prefixs = Compact(prefix)
    //val (hasMore, keys) = client.getParent(databaseName, tableName, prefix, itemCount, lowKey, highKey)
    if (highKey != null) {
      hasBefore = hasMore
      hasAfter = true
    } else if (lowKey != null) {
      hasBefore = true
      hasAfter = hasMore
    } else {
      hasBefore = false
      hasAfter = hasMore
    }
    setTable(databaseName, tableName)
    left.setName("Tree:" + prefixs)
    left.setUpDown(
        if (hasBefore) { _ => {highKey = lowKey; lowKey = null; toTree(databaseName, tableName, prefix, false)} } else { null },
        if (hasAfter) { _ => {lowKey = highKey; highKey = null; toTree(databaseName, tableName, prefix, false)} } else { null }
          )
    left.setAct((key: String) => {
      if (key != null) {
        if (key == prefixs) {
          // Current
          toItem(databaseName, tableName, prefix)
        } else if (key == "..") {
          // Parent
          if (prefix.size() > 0) {
            val prefix1 = prefix.dropRight(1)
            toTree(databaseName, tableName, prefix1,true)
          }
        } else {
          // Child
          val prefix1 = jgetArray(Json(key))
          lowKey = null
          highKey = null
          val (hasMore1, keys1) = client.getParent(databaseName, tableName, prefix1, itemCount, lowKey, highKey)
          if (jsize(keys1) > 0) {
            toTree1(databaseName, tableName, prefix1, true, hasMore, keys1)
          } else {
            toItem(databaseName, tableName, Json(key))
          }
        }
      }
    },
      _ => { // No add button
      })
    left.clear()
    lowKey = null
    highKey = null
    var first = true
    for (key <- jgetArray(keys)) {
      if (first) {
        if (prefix.size() > 0) left.add("..")
        if (! hasBefore) left.add(prefixs)
        if (first) lowKey = key
      }
      highKey = key
      left.add(Compact(key))
      first = false
    }
    //
    left.list.select(prefixs)
  }

  def toTree(databaseName: String, tableName: String, prefix: JsonArray,reset:Boolean) {
    if (reset) {
      lowKey = null
      highKey = null
    }
    val (hasMore, keys) = client.getParent(databaseName, tableName, prefix, itemCount, lowKey, highKey)
    toTree1(databaseName, tableName, prefix, reset, hasMore, keys)
  }

  def toNodes(databaseName: String, ringName: String) {
    val nodes = client.getNodes(databaseName, ringName)
    setRing(databaseName, ringName)
    left.setName("Node")
    left.setAct((nodeName: String) => {
      if (nodeName != null) {
        toNode(databaseName, ringName, nodeName)
      }
    },
      _ => {})
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
    dbTables.setVisible(status == "active")
    dbRings.setVisible(status == "active")
    dbServers.setVisible(status == "active")
    dbStop.setVisible(status == "active")
    dbStart.setVisible(status == "stop")
    dbDelete.setVisible(status == "stop")
    right.setName("Database: " + databaseName)
    right.setMode("database")
  }

  def toTable(databaseName: String, tableName: String) {
    this.databaseName = databaseName
    this.tableName = tableName
    right.setName("Table: " + tableName)
    right.setMode("table")
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
    right.ta.setValue(Pretty(v))
    right.ta.setReadOnly(true)

    right.vta.setReadOnly(false)
    //right.vta.setValue(Pretty(cv))
    right.vta.setValue(vcToString(cv))
    right.vta.setReadOnly(true)
  }

  def toNode(databaseName: String, ringName: String, nodeName: String) {
    this.databaseName = databaseName
    this.ringName = ringName
    this.nodeName = nodeName
    right.setName("Node: " + nodeName)
    right.setMode("node")
  }
}

class OStoreUI extends Application {
  // TODO list paged fetch for long lists
  def init(): Unit = {
    setTheme("runo")

    val top = new Top
    val left = new Left
    val right = new Right
    val all = new All(top.all, left.all, right.all)
    setMainWindow(all.all)

    val client = new WebClient()
    val act = new Act(this, client, all, top, left, right)

    // Database
    act.toDatabases()

  }

}