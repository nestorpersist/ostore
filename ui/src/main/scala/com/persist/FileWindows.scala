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
//import scala.actors.Future
import java.io.FileOutputStream
import java.io.File
import Exceptions._
import akka.actor.ActorSystem
import akka.dispatch.Future

object FileWindows {

  class Loader(system: ActorSystem, put: (Json, Json) => Future[Unit], done: => Unit, error: Label)
    extends Upload.Receiver with Upload.SucceededListener with Upload.FailedListener with Upload.StartedListener {

    var loader: BulkImpl.Load = null
    var fileName: String = ""

    def uploadStarted(event: Upload.StartedEvent) {
    }

    def receiveUpload(filename: String, MIMEType: String): OutputStream = {
      this.fileName = filename
      val inStream = new PipedInputStream()
      val outStream = new PipedOutputStream(inStream)
      loader = new BulkImpl.Load(system, new BulkImpl.Split(inStream), put, 10)
      outStream
    }

    def uploadSucceeded(event: Upload.SucceededEvent) {
      try {
        loader.waitDone
        done
      } catch {
        case ex: JsonParseException => error.setValue(fileName + ": " + ex.shortString)
        case ex: Throwable => error.setValue(fileName + ": " + ex.toString)
      }
    }

    def uploadFailed(event: Upload.FailedEvent) {
      println("upload fail")
    }
  }

  class Receiver(act: Act)
    extends Upload.Receiver with Upload.SucceededListener with Upload.FailedListener with Upload.StartedListener {

    var reader: BufferedReader = null
    var ireader: InputStreamReader = null
    var fileName: String = ""

    def uploadStarted(event: Upload.StartedEvent) {
    }

    def receiveUpload(filename: String, MIMEType: String): OutputStream = {
      // TODO buffer may not be big enough - run act.read on sep thread!
      this.fileName = filename
      val inStream = new PipedInputStream(100000000)
      val outStream = new PipedOutputStream(inStream)
      ireader = new InputStreamReader(inStream)
      reader = new BufferedReader(new InputStreamReader(inStream))
      outStream
    }

    def uploadSucceeded(event: Upload.SucceededEvent) {
      // TODO read should run in a separate thread with pipe of bounded size
      act.read(fileName, reader)
    }

    def uploadFailed(event: Upload.FailedEvent) {
      println("upload fail")
    }
  }

  trait Act {
    def read(fineName: String, reader: BufferedReader): Unit
  }

  class Create(ta: TextField, error: Label, client: WebClient, exit: () => Unit) extends Act {

    private def databaseExists(databaseName: String): Boolean = {
      val databases = client.getDatabases()
      for (database <- JsonArray(databases)) {
        if (databaseName == database) return true
      }
      return false

    }
    def read(fileName: String, reader: BufferedReader): Unit = {
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
      reader.close()
      var config = b.toString()
      val jconfig = try {
        Json(config)
      } catch {
        case ex: JsonParseException => {
          val msg = ex.shortString()
          error.setValue(fileName + ": " + msg)
          return
        }
      }
      try {
        client.configAct("create", database, jconfig)
      } catch {
        case ex: SystemException => {
          error.setValue(ex.toString())
          return
        }
      }
      exit()
    }
  }

  class Change(add: Boolean, database: String, kind: String, error: Label, client: WebClient, exit: () => Unit) extends Act {
    def read(fileName: String, reader: BufferedReader): Unit = {
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
      reader.close()
      var config = b.toString()
      val jconfig = try {
        Json(config)
      } catch {
        case ex: JsonParseException => {
          val msg = ex.shortString()
          error.setValue(fileName + ": " + msg)
          return
        }
      }
      val cmd = (if (add) "add" else "delete") ++ (kind ++ "s")
      try {
        client.configAct(cmd, database, jconfig)
      } catch {
        case ex: SystemException => {
          error.setValue(ex.toString())
          return
        }
      }
      exit()
    }
  }

  def load(system: ActorSystem, w: Window, databaseName: String, tableName: String, client: WebClient) {
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

    implicit val executor = system.dispatcher
    def put(key: Json, value: Json) = Future { client.putItem(databaseName, tableName, key, value) }
    def done = w.removeWindow(fileWin)
    val r = new Loader(system, put, done, error)

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

  def change(add: Boolean, database: String, kind: String, w: Window, client: WebClient, act: () => Unit) {
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

    //val ta = new TextField("Database Name")
    //c.addComponent(ta)

    val error = new Label("")
    c.addComponent(error)

    val r = new Receiver(new Change(add, database, kind, error, client, () => { w.removeWindow(fileWin); act() }))

    val upload = new Upload(kind + "s Configuration", r)
    val label = if (add) "Add" else "Delete"
    upload.setButtonCaption(label + " " + kind + "s")
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