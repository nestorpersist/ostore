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

// TODO change icon

private[persist] trait UIAssembly extends ActComponent with ButtonsComponent with LayoutComponent
  with EditComponent

private[persist] class OStoreUI extends Application {
  def app = this
  def init(): Unit = {
    val client = new WebClient()
    setTheme("runo")

    object UIAll extends UIAssembly {
      val act = new UIAct(client)
      val top = new Top
      val left = new Left
      val right = new Right
      val all = new All(top.all, left.all, right.all)
      val buttons = new Buttons(client, app)
      val editWindow = new EditWindow
    }

    setMainWindow(UIAll.all.all)

    UIAll.act.toDatabases()
  }
}
