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
import com.vaadin.terminal.StreamResource
import java.io.InputStream
import java.io.ByteArrayInputStream
import JsonOps._

private[persist] class StringResource(s: String) extends StreamResource.StreamSource {
  def getStream(): InputStream = {
    new ByteArrayInputStream(s.getBytes("UTF-8"))
  }
}

private[persist] trait ButtonsComponent { this: UIAssembly =>
  val buttons: Buttons

  class Buttons(client: WebClient, app: Application) {

    // Top Buttons

    val homeButton = new Button("Home")
    homeButton.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        act.toDatabases()
      }
    })

    val databaseButton = new Button("")
    databaseButton.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        act.toDatabases()
        left.list.select(act.databaseName)
        act.toDatabase(act.databaseName)
      }
    })
    val tableButton = new Button("")
    tableButton.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        act.toTables(act.databaseName)
        left.list.select(act.tableName)
        act.toTable(act.databaseName, act.tableName)
      }
    })
    val ringButton = new Button("")
    ringButton.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        act.toRings(act.databaseName)
        left.list.select(act.ringName)
        act.toRing(act.databaseName, act.ringName)
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
        act.toTables(act.databaseName)
      }
    })
    val dbRings = new Button("Show Rings")
    dbRings.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        act.toRings(act.databaseName)
      }
    })
    val dbServers = new Button("Show Servers")
    dbServers.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        act.toServers(act.databaseName)
      }
    })
    val dbStop = new Button("Stop Database")
    dbStop.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        client.stopDatabase(act.databaseName)
        act.toDatabase(act.databaseName)
      }
    })
    val dbStart = new Button("Start Database")
    dbStart.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        client.startDatabase(act.databaseName)
        act.toDatabase(act.databaseName)
      }
    })
    val dbDelete = new Button("Delete Database")
    dbDelete.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        client.deleteDatabase(act.databaseName)
        act.toDatabases()
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
        act.toKeys(act.databaseName, act.tableName, true)
      }
    })
    val t2 = new Button("Show Tree")
    t2.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        act.toTree(act.databaseName, act.tableName, JsonArray(), true)
      }
    })
    val t3 = new Button("Delete Table")
    t3.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        client.deleteTable(act.databaseName, act.tableName)
        act.toTables(act.databaseName)
      }
    })
    val t4 = new Button("Upload")
    t4.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        FileWindows.load(all.all, act.databaseName, act.tableName, client)
      }
    })
    val t5 = new Button("Download")
    t5.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        // TODO should stream in for large tables
        // TODO deal with paging for large tables
        val items = client.getItems(act.databaseName, act.tableName, act.itemCount + 1)
        var result = ""
        for (item <- jgetArray(items)) {
          result += Compact(jget(item, "k")) + "\t" + Compact(jget(item, "v")) + "\n"
        }
        val sr = new StreamResource(new StringResource(result), act.databaseName + "." + act.tableName + ".json", app)
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
        def finish(delete: Boolean) = {
          if (delete) {
            client.deleteItem(act.databaseName, act.tableName, act.key)
          }
          act.toKeys(act.databaseName, act.tableName, false)
        }
        val ok = client.conditionalDeleteItem(act.databaseName, act.tableName, act.key, act.cv)
        if (ok) {
          finish(false)
        } else {
          editWindow.test(finish, all.all, "Conflict Detected", "Item has changed", "Delete", "Cancel")
        }
      }
    })
    val i2 = new Button("Edit Item")
    i2.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        editWindow.edit(all.all, act.databaseName, act.tableName, act.key, act.cv, act.itemVal, client)
      }
    })
    right.itemButtons.addComponent(i1)
    right.itemButtons.addComponent(i2)

    // Right Ring Buttons
    val r1 = new Button("Show Nodes")
    r1.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        act.toNodes(act.databaseName, act.ringName)
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
  }
}