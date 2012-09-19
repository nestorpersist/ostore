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

import com.vaadin.ui._
import com.vaadin.ui.Button.ClickListener
import com.vaadin.terminal.Sizeable
import com.vaadin.ui.themes._
import com.vaadin.data.Property
import JsonOps._

private[persist] trait LayoutComponent { this: UIAssembly =>
  val top: Top
  val left: Left
  val right: Right
  val all: All

  class Top {
    val all = new HorizontalLayout()
  }

  class Left {
    private var selectAct: String => Unit = { (s: String) => { println(s) } }
    private var addAct: Unit => Unit = { _ => println("ADD***") }
    private var upAct: Unit => Unit = null
    private var downAct: Unit => Unit = null
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

    def setName(name: String, hasAdd:Boolean) {
      if (name.contains(":")) {
        all.setCaption(name)
      } else {
        all.setCaption(name + "s")
        add.setCaption("Add " + name ++ (if(name == "Node" || name == "Ring") "s" else ""))
      }
      add.setVisible(hasAdd)
      up.setVisible(name == "Item" || name.startsWith("Tree:"))
      down.setVisible(name == "Item" || name.startsWith("Tree:"))
    }

    def setUpDown(upAct: Unit => Unit, downAct: Unit => Unit) {
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

    val ro = new Label("")
    r1.addComponent(ro)

    val map = new TextArea("Map")
    map.setSizeFull()
    r1.addComponent(map)
    map.setVisible(false)
    
    val reduce = new TextArea("Reduce")
    reduce.setSizeFull()
    r1.addComponent(reduce)
    reduce.setVisible(false)

    val ta = new TextArea("Value")
    ta.setSizeFull()
    r1.addComponent(ta)
    ta.setVisible(false)

    val vta = new TextArea("Vector Clock")
    vta.setSizeFull()
    r1.addComponent(vta)
    vta.setVisible(false)
    
    def setName(name: String) {
      all.setCaption(name)
    }
    def setStatus(stat: String) {
      status.setValue("Status: " + stat)
    }
    def setReadOnly(readOnly :Boolean, toinfo :Json) {
      ro.setValue("ReadOnly: " + readOnly)
      val info = jgetObject(toinfo)
      if (readOnly) {
        if (info.get("map") != None) {
          map.setVisible(true)
          map.setReadOnly(false)
          map.setValue(Pretty(jget(info,"map")))
          map.setReadOnly(true)
        }
        if (info.get("reduce") != None)
          reduce.setVisible(true)
          reduce.setReadOnly(false)
          reduce.setValue(Pretty(jget(info,"reduce")))
          reduce.setReadOnly(true)
      }
    }
    def setMode(mode: String) {
      databaseButtons.setVisible(mode == "database")
      status.setVisible(mode == "database")
      tableButtons.setVisible(mode == "table")
      itemButtons.setVisible(mode == "item")
      ringButtons.setVisible(mode == "ring")
      nodeButtons.setVisible(mode == "node")
      ro.setVisible(mode == "table")
      map.setVisible(false)
      reduce.setVisible(false)
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
}
