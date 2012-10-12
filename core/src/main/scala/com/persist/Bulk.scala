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
import Exceptions._
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.io.InputStreamReader
import java.io.ByteArrayInputStream
import java.io.FileInputStream
import akka.actor.ActorSystem
import akka.dispatch.Future
import java.io.FileOutputStream
import java.io.ByteArrayOutputStream

/**
 * The Bulk loader moves multiple items in and out
 * of OStore tables.
 *
 * The external form used is a sequence of lines.
 * Each line specifies a single item and
 * consists of the item JSON key followed by a tab followed by the
 * item JSON value.
 *
 * This is the same format used by the UI Upload and Download operations.
 */
object Bulk {

  private val fast = JsonObject("fast" -> true)

  /**
   * Uploads items into a table.
   *
   * @param in the source of items.
   *   - File. Items are in a file.
   *   - String. Items are in string.
   *   - InputStream. Stream containing items.
   *   - Table or AsyncTable. Get all items from this table. Used to copy items between tables.
   *   - Iterable[(Json,Json)]. A sequence of key-value pairs. Allows you to program your own source.
   * @param out the destination of items.
   *   - Table or AsyncTable. There is no difference in behavior. Use whichever is easiest.
   * @param cnt the number of parallel "put" operations. The default is 5.
   */
  def upload(in: Any, out: Any, cnt: Int = 5) {
    val in1: Iterable[(Json, Json)] = in match {
      case f: File => new BulkImpl.Split(new FileInputStream(f))
      case s: String => new BulkImpl.Split(new ByteArrayInputStream(s.getBytes("UTF-8")))
      case s: InputStream => new BulkImpl.Split(s)
      case table: Table => table.all(JsonObject("get" -> "kv")).map(v => (jget(v, "k"), jget(v, "v")))
      case asyncTable: AsyncTable => {
        val table = asyncTable.database.table(asyncTable.tableName)
        table.all(JsonObject("get" -> "kv")).map(v => (jget(v, "k"), jget(v, "v")))
      }
      case i: Iterable[(Json, Json)] => i
      case x => throw new SystemException(Codes.BadRequest,"Bad upload input")
    }
    val (system: ActorSystem, out1: ((Json, Json) => Future[Unit])) = out match {
      case asyncTable: AsyncTable => (asyncTable.database.client.system, (key: Json, value: Json) => asyncTable.put(key, value, fast))
      case table: Table => {
        val asyncTable = table.database.asyncTable(table.tableName)
        (asyncTable.database.client.system, (key: Json, value: Json) => asyncTable.put(key, value, fast))
      }
      case x => throw new SystemException(Codes.BadRequest,"Bad upload output")
    }
    val impl = new BulkImpl.Load(system, in1, out1, cnt)
    impl.waitDone
  }

  /**
   * Downloads items from a table.
   *
   * @param in the source of the items.
   *   - Table or AsyncTable. Get all items from this table. There is no difference in behavior. Use whichever is easiest.
   *   - Iterable[(Json,Json)]. A sequence of key-value pairs. Allows you to program your own source.
   * @param out the destination of items.
   *   - File. writes items to a file.
   *   - OutputStream. writes items to a stream.
   */
  def download(in: Any, out: Any) {
    val in1: Iterable[(Json, Json)] = in match {
      case table: Table => table.all(JsonObject("get" -> "kv")).map(v => (jget(v, "k"), jget(v, "v")))
      case asyncTable: AsyncTable => {
        val table = asyncTable.database.table(asyncTable.tableName)
        table.all(JsonObject("get" -> "kv")).map(v => (jget(v, "k"), jget(v, "v")))
      }
      case i: Iterable[(Json, Json)] => i
      case x => throw new SystemException(Codes.BadRequest,"Bad download input")
    }
    val out1 = out match {
      case s: OutputStream => s
      case f: File => new FileOutputStream(f)
      case x => throw new SystemException(Codes.BadRequest,"Bad download output")
    }
    BulkImpl.download(in1, out1)
  }

  /**
   * Downloads items from a table into a string.
   *
   * @param in the source of the items.
   *   - Table or AsyncTable. Get all items from this table. There is no difference in behavior. Use whichever is easiest.
   *   - Iterable[(Json,Json)]. A sequence of key-value pairs. Allows you to program your own source.
   * @return the string containing the items.
   */
  def download(in: Any): String = {
    val b = new ByteArrayOutputStream()
    download(in, b)
    b.toString("UTF-8")
  }

}