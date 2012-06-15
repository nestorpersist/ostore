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
import com.vaadin.ui.themes._
import JsonUtil._
import scala.collection.JavaConversions._
import com.vaadin.data.Property
import java.util.regex.Pattern
import com.twitter.json.JsonException

object EditWindow {
  private def checkJson(err: Label, s: String, isKey: Boolean): Option[Json] = {
    err.setValue("")
    try {
      Some(Json(s))
    } catch {
      case ex: JsonException => {
        err.setValue(ex.getMessage())
        None
      }
      case ex1: Exception => {
        err.setValue(ex1.toString())
        None
      }
    }
  }

  private def checkKey(k: String): Option[Json] = {
    try {
      Some(keyDecode(k))
    } catch {
      case ex: Exception => None
      case ex: java.lang.Exception => None
    }
  }

  private def checkBoth(err: Label, k: String, s: String): Option[(Json, Json)] = {
    checkJson(err, k, true) match {
      case Some(jk: Json) => {
        try {
          keyEncode(jk)
          checkJson(err, s, false) match {
            case Some(jv: Json) => {
              Some(jk, jv)
            }
            case None => None
          }
        } catch {
          case ex: Exception => {
            err.setValue("Bad key form")
            None
          }
        }
      }
      case None => None
    }
  }

  private def popup(act: Act, w: Window, databaseName: String, tableName: String,
    key: JsonKey, value: Json, client: WebClient, add: Boolean) {
    val title = if (add) {
      "Add New Item"
    } else {
      "Edit Item: " + Compact(key)
    }
    val editWin = new Window(title)
    editWin.getContent().setSizeFull()
    editWin.setReadOnly(true)
    val c = new VerticalLayout()
    c.setSizeFull()
    editWin.addComponent(c)
    val h1 = w.getHeight()
    val h1u = w.getHeightUnits()
    val w1 = w.getWidth()
    val w1u = w.getWidthUnits()
    editWin.setPositionX(100)
    editWin.setPositionY(100)
    editWin.setHeight(h1 * 0.6f, h1u)
    editWin.setWidth(w1 * 0.6f, w1u)
    val buttons = new HorizontalLayout()
    val b1 = new Button("Format")
    buttons.addComponent(b1)
    val b2 = new Button("Check Syntax")
    buttons.addComponent(b2)
    val b3 = new Button(if (add) "Add" else "Save")
    buttons.addComponent(b3)
    val b4 = new Button("Cancel")
    buttons.addComponent(b4)
    c.addComponent(buttons)
    val error = new Label("")
    c.addComponent(error)

    val keyTa = new TextArea("Key")
    if (add) {
      keyTa.setSizeFull()
      c.addComponent(keyTa)
      c.setExpandRatio(keyTa, 0.2F)
    }

    val editTa = new TextArea("Value")
    editTa.setSizeFull()
    editTa.setValue(if (add) "" else Pretty(value))
    c.addComponent(editTa)
    c.setExpandRatio(editTa, 1.0F)

    w.addWindow(editWin)
    b1.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        val v = editTa.getValue().asInstanceOf[String]
        checkJson(error, v, false) match {
          case Some(j) => editTa.setValue(Pretty(j))
          case None =>
        }
      }
    })
    b2.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        val v = editTa.getValue().asInstanceOf[String]
        if (add) {
          val k = keyTa.getValue().asInstanceOf[String]
          checkBoth(error, k, v)
        } else {
          checkJson(error, v, false)
        }
      }
    })
    if (add) {
      b3.addListener(new ClickListener {
        def buttonClick(e: Button#ClickEvent) = {
          val k = keyTa.getValue().asInstanceOf[String]
          val v = editTa.getValue().asInstanceOf[String]
          checkBoth(error, k, v) match {
            case Some((jk, jv)) => {
              w.removeWindow(editWin)
              def finish(save:Boolean) = {
                if (save) {
                  client.putItem(databaseName, tableName, jk, jv)
                }
                act.toKeys(databaseName, tableName, false) // reload key list
                act.toItem(databaseName, tableName, jk)
              } 
              val ok = client.addItem(databaseName, tableName, jk, jv)
              if (!ok) {
                test(finish, w,"Conflict Detected","Item already exists","Overwrite","Cancel")
              } else {
                finish(false)
              }
            }
            case None =>
          }
        }
      })
    } else {
      b3.addListener(new ClickListener {
        def buttonClick(e: Button#ClickEvent) = {
          val v = editTa.getValue().asInstanceOf[String]
          checkJson(error, v, false) match {
            case Some(j) => {
              w.removeWindow(editWin)
              client.putItem(databaseName, tableName, key, j)
              act.toItem(databaseName, tableName, key)
            }
            case None =>
          }
        }
      })

    }
    b4.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        w.removeWindow(editWin)
      }
    })
  }

  def test(finish:(Boolean)=>Unit,w: Window, title: String, msg: String, yes: String, no: String) {
    val testWin = new Window(title)
    testWin.getContent().setSizeFull()
    testWin.setReadOnly(true)
    val c = new VerticalLayout()
    c.setSizeFull()
    testWin.addComponent(c)
    val h1 = w.getHeight()
    val h1u = w.getHeightUnits()
    val w1 = w.getWidth()
    val w1u = w.getWidthUnits()
    testWin.setPositionX(100)
    testWin.setPositionY(100)
    testWin.setHeight(h1 * 0.6f, h1u)
    testWin.setWidth(w1 * 0.6f, w1u)
    val ta = new TextField(msg)
    c.addComponent(ta)
    val buttons = new HorizontalLayout()
    val b1 = new Button(yes)
    buttons.addComponent(b1)
    val b2 = new Button(no)
    buttons.addComponent(b2)
    c.addComponent(buttons)
    w.addWindow(testWin)
    b2.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        w.removeWindow(testWin)
        finish(true)
      }
    })
    b2.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        w.removeWindow(testWin)
        finish(false)
      }
    })
  }

  def edit(act: Act, w: Window, databaseName: String, tableName: String, key: JsonKey, value: Json, client: WebClient) {
    popup(act, w, databaseName, tableName, key, value, client, false)
  }

  def add(act: Act, w: Window, databaseName: String, tableName: String, client: WebClient) {
    popup(act, w, databaseName, tableName, null, null, client, true)
  }

}