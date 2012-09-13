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
import JsonOps._
import scala.collection.JavaConversions._
import com.vaadin.data.Property
import java.util.regex.Pattern
import java.io.OutputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import scala.actors.Future
import java.io.FileOutputStream
import java.io.File

object FileWindows {

  class Receiver(act: Act)
    extends Upload.Receiver with Upload.SucceededListener with Upload.FailedListener with Upload.StartedListener {

    var reader: BufferedReader = null
    var ireader: InputStreamReader = null

    def uploadStarted(event: Upload.StartedEvent) {
      //println("*1")
    }

    def receiveUpload(filename: String, MIMEType: String): OutputStream = {
      // TODO buffer may not be big enough - run act.read on sep thread!
      val inStream = new PipedInputStream(100000000)
      val outStream = new PipedOutputStream(inStream)
      ireader = new InputStreamReader(inStream)
      reader = new BufferedReader(new InputStreamReader(inStream))
      //println("*2")
      outStream
    }

    def uploadSucceeded(event: Upload.SucceededEvent) {
      // TODO read should run in a separate thread with pipe of bounded size
      //println("*3")
      act.read(reader)
      //println("*4")
    }

    def uploadFailed(event: Upload.FailedEvent) {
      println("upload fail")
    }
  }

  trait Act {
    def read(reader: BufferedReader): Unit
  }

  class Reader(databaseName: String, tableName: String, exit: () => Unit, error: Label, client: WebClient) extends Act {

    private def doItem(item: String): String = {
      val parts = item.split("\t")
      if (parts.size >= 2) {
        try {
          val key = Json(parts(0))
          val value = Json(parts(1))
          client.putItem(databaseName, tableName, key, value)
          ""
        } catch {
          case ex: Exception => {
            //val msg = ex.getMessage()
            val msg = ex.toString()
            msg + ": " + item
          }
          case x => "unknown: "+ x.toString()
        }
      } else {
        "no tab" + ": " + item
      }
    }

    def read(reader: BufferedReader): Unit = {
      var line = 0
      var msg = ""
      var done = false
      while (!done) {
        line += 1
        //val ch = ireader.read()
        val s = reader.readLine()
        if (s == null) {
          done = true
        } else {
          val error = doItem(s)
          if (error != "") {
            msg = "[Line:" + line + "] " + error
            done = true
          }
        }
      }
      reader.close()
      if (msg == "") {
        exit()
      } else {
        error.setValue(msg)
      }
    }

  }

  class Create(ta: TextField, error: Label, client: WebClient, exit: () => Unit) extends Act {

    private def databaseExists(databaseName: String): Boolean = {
      val databases = client.getDatabases()
      for (database <- JsonArray(databases)) {
        if (databaseName == database) return true
      }
      return false

    }
    def read(reader: BufferedReader): Unit = {
      val database = ta.getValue().asInstanceOf[String]
      if (databaseExists(database)) {
        error.setValue("database " + database + " already exists")
        return
      }
      var done = false
      var b = new StringBuilder()
      while (!done) {
        val s = reader.readLine()
        if (s == null) {
          done = true
        } else {
          b.append(s + "\n")
        }
      }
      var config = b.toString()
      //println(config)
      val jconfig = try {
        Json(config)
      } catch {
        case ex: Exception => {
          val msg = ex.getMessage()
          error.setValue(msg)
          return
        }
      }
      client.createDatabase(database, jconfig)
      reader.close()
      exit()
    }
  }

  def load(w: Window, databaseName: String, tableName: String, client: WebClient) {
    val fileWin = new Window("Bulk Load Items for /" + databaseName + "/" + tableName)
    fileWin.getContent().setSizeFull()
    fileWin.setReadOnly(true)
    val c = new VerticalLayout()
    fileWin.addComponent(c)
    val h1 = w.getHeight()
    val h1u = w.getHeightUnits()
    val w1 = w.getWidth()
    val w1u = w.getWidthUnits()
    fileWin.setPositionX(100)
    fileWin.setPositionY(100)
    fileWin.setHeight(h1 * 0.6f, h1u)
    fileWin.setWidth(w1 * 0.6f, w1u)

    val buttons = new HorizontalLayout()
    buttons.setSizeFull()

    val error = new Label("")
    c.addComponent(error)

    val r = new Receiver(new Reader(databaseName, tableName, () => w.removeWindow(fileWin), error, client))

    val upload = new Upload("", r)
    upload.setButtonCaption("Load Now")
    upload.addListener(r.asInstanceOf[Upload.SucceededListener])
    upload.addListener(r.asInstanceOf[Upload.FailedListener])
    upload.addListener(r.asInstanceOf[Upload.StartedListener])
    buttons.addComponent(upload)

    val x = new Label("")
    buttons.addComponent(x)
    buttons.setExpandRatio(x, 1.0f)

    val cancel = new Button("Cancel")
    buttons.addComponent(cancel)
    c.addComponent(buttons)

    w.addWindow(fileWin)

    cancel.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        w.removeWindow(fileWin)
      }
    })
  }

  def createDatabase(w: Window, client: WebClient, act: () => Unit) {
    val fileWin = new Window("Create Database")
    fileWin.getContent().setSizeFull()
    fileWin.setReadOnly(true)
    val c = new VerticalLayout()
    fileWin.addComponent(c)
    val h1 = w.getHeight()
    val h1u = w.getHeightUnits()
    val w1 = w.getWidth()
    val w1u = w.getWidthUnits()
    fileWin.setPositionX(100)
    fileWin.setPositionY(100)
    fileWin.setHeight(h1 * 0.6f, h1u)
    fileWin.setWidth(w1 * 0.6f, w1u)

    val buttons = new HorizontalLayout()
    buttons.setSizeFull()

    val ta = new TextField("Database Name")
    c.addComponent(ta)

    val error = new Label("")
    c.addComponent(error)

    val r = new Receiver(new Create(ta, error, client, () => { w.removeWindow(fileWin); act() }))

    val upload = new Upload("Database Configuration", r)
    upload.setButtonCaption("Create Database")
    upload.addListener(r.asInstanceOf[Upload.SucceededListener])
    upload.addListener(r.asInstanceOf[Upload.FailedListener])
    upload.addListener(r.asInstanceOf[Upload.StartedListener])
    buttons.addComponent(upload)

    val x = new Label("")
    buttons.addComponent(x)
    buttons.setExpandRatio(x, 1.0f)

    val cancel = new Button("Cancel")
    buttons.addComponent(cancel)
    c.addComponent(buttons)

    w.addWindow(fileWin)

    cancel.addListener(new ClickListener {
      def buttonClick(e: Button#ClickEvent) = {
        w.removeWindow(fileWin)
      }
    })
  }
}