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

import akka.actor.Actor
import akka.dispatch.Future
import Actor._
import JsonOps._
import JsonKeys._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.DefaultPromise
import akka.util.duration._
import akka.dispatch._
import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import Exceptions._
import ExceptionOps._

/**
 * This is the asynchronous interface to OStore tables.
 * Instances of this class are created by the [[com.persist.Database]] asyncTable method.
 *
 * @param database the enclosing database.
 * @param tableName the name of the table.
 */
class AsyncTable private[persist] (val database:Database, val tableName: String, system: ActorSystem, send: ActorRef) {
  /**
   * The name of the database.
   */
  val databaseName = database.databaseName

  private implicit val executor = system.dispatcher

  private def put1(key: JsonKey, value: Json, vectorClock: Option[Json],
    options: JsonObject, create: Boolean): Future[Boolean] = {
    val ring = jgetString(options, "ring")
    val options1 = options - "ring"
    var request = JsonObject()
    vectorClock match {
      case Some(c) => request = request + ("c" -> c)
      case None =>
    }
    if (jsize(options1) > 0) request = request + ("o" -> options1)
    if (create) request = request + ("create" -> true)
    var f1 = new DefaultPromise[Any]
    send ! ("put", ring, f1, tableName, keyEncode(key), (Compact(value), Compact(request)))
    val f2 = f1.map { x =>
      {
        val (code: String, v1: String) = x
        if (code == Codes.Conflict) {
          false
        } else {
          checkCode(code, v1)
          true
        }
      }
    }
    f2
  }

  /**
   * Modifies the value of an item. If the item does not exist it is created.
   *
   * @param key the key.
   * @param value the new value.
   * @param options optional json object containing options.
   *  - '''"w"=n''' write at least n rings before returning. Default is 1.
   *  - '''"fast"=true''' if true, returns when the item has been updated in server memory.
   *        If false, returns only after item has also been written to server disk. Default is false.
   *  - '''"ring"="ringName"''' write to this ring.
   *  - '''"expires"=t''' the millisecond time at which this item expires.
   *
   *  @return a future whose completion indicates that the put has been handled by the server.
   */
  def put(key: JsonKey, value: Json, options: JsonObject = emptyJsonObject): Future[Unit] = {
    val f1 = put1(key, value, None, options, false)
    f1.map { b => }
  }

  /**
   * Provides optimistic concurrency control.
   * Modifies the value of an item only if its vector clock on the server matches
   * the vector clock passed in.
   *
   * @param key the key.
   * @param value the new value.
   * @param vectorClock the vector clock to match. This would typically have been returned by a previous get
   * call.
   * @param options optional json object containing options.
   *  - '''"w"=n''' write at least n rings before returning. Default is 1.
   *  - '''"fast"=true''' if true, returns when the item has been updated in server memory.
   *        If false, returns only after item has also been written to server disk. Default is false.
   *  - '''"ring"="ringName"''' write to this ring.
   *  - '''"expires"=t''' the millisecond time at which this item expires.
   *
   *  @return a future whose value upon completion is
   *  true if the item was modified (vector clock matched). False if item was not modified (vector
   *  clock did not match).
   */
  def conditionalPut(key: JsonKey, value: Json, vectorClock: Json, options: JsonObject = emptyJsonObject): Future[Boolean] = {
    put1(key, value, Some(vectorClock), options, false)
  }

  /**
   * Creates an item only if it does not already exist.
   *
   * @param key the key.
   * @param value the new value.
   * @param options optional json object containing options.
   *  - '''"w"=n''' write at least n rings before returning. Default is 1.
   *  - '''"fast"=true''' if true, returns when the item has been updated in server memory.
   *        If false, returns only after item has also been written to server disk. Default is false.
   *  - '''"ring"="ringName"''' write to this ring.
   *  - '''"expires"=t''' the millisecond time at which this item expires.
   *
   *  @return a future whose value upon completion is
   *  true if the item was created. False if the item was not modified because it already exists.
   */
  def create(key: JsonKey, value: Json, options: JsonObject = emptyJsonObject): Future[Boolean] = {
    put1(key, value, None, options, true)
  }

  /**
   *
   * Deletes an item. Has no effect if the item does not exist
   *
   * @param key the key.
   * @param options optional json object containing options.
   *  - '''"w"=n''' write at least n rings before returning. Default is 1.
   *  - '''fast=true''' if true, returns when the item has been updated in server memory.
   *        If false, returns only after item has also been written to server disk. Default is false.
   *  - '''"ring"="ringName"''' write to this ring.
   *
   *  @return a future whose completion indicates that the delete has been handled by the server.
   *
   */
  def delete(key: JsonKey, options: JsonObject = emptyJsonObject): Future[Unit] = {
    val ring = jgetString(options, "ring")
    val options1 = options - "ring"
    val request = JsonObject("o" -> options1)
    var f1 = new DefaultPromise[Any]
    send ! ("delete", ring, f1, tableName, keyEncode(key), Compact(request))
    val f2 = f1.map { x =>
      {
        val (code: String, v1: String) = x
        checkCode(code, v1)
      }
    }
    f2
  }

  /**
   *
   * Provides optimistic concurrency control.
   * Deletes the item only if its vector clock on the server matches
   * the vector clock passed in.
   *
   * @param key the key.
   * @param vectorClock the vector clock to match. This would typically have been returned by a previous get
   * call.
   * @param options optional json object containing options.
   *  - '''"w"=n''' write at least n rings before returning. Default is 1.
   *  - '''fast=true''' if true, returns when the item has been updated in server memory.
   *        If false, returns only after item has also been written to server disk. Default is false.
   *  - '''"ring"="ringName"''' write to this ring.
   *
   *  @return a future whose value upon completion is
   *  true if the item was deleted (vector clock matched). False if item was not deleted (vector
   *  clock did not match).
   *
   */
  def conditionalDelete(key: JsonKey, vectorClock: Json, options: JsonObject = emptyJsonObject): Future[Boolean] = {
    val ring = jgetString(options, "ring")
    val options1 = options - "ring"
    val request = JsonObject("c" -> vectorClock, "o" -> options1)
    var f1 = new DefaultPromise[Any]
    send ! ("delete", ring, f1, tableName, keyEncode(key), Compact(request))
    val f2 = f1.map { x =>
      {
        val (code: String, v1: String) = x
        if (code == Codes.Conflict) {
          false
        } else {
          checkCode(code, v1)
          true
        }
      }
    }
    f2
  }

  /**
   * Gets information about an item with a specified key.
   *
   * @param key the key.
   * @param options optional json object containing options.
   *  - '''"get="kvcde"''' if specified this method returns an object with requested fields
   *      (Key, Value, vector Clock, Deleted, Expires time).
   *  - '''"r"=n''' read from at least n rings before returning. Default is 1.
   *  - '''"ring"="ringName"''' get from this ring.
   *  - '''"prefixtab"="prefixName"''' get items from this prefix table rather than the main table.
   *
   * @return A future that upon completion will have value
   * None if there is no item with that key, or Some(X) if the item exists.
   * X will be the value of the item if the get option is not specified;
   * otherwise, an object with the fields specified in the get option.
   */
  def get(key: JsonKey, options: JsonObject = emptyJsonObject): Future[Option[Json]] = {
    val ring = jgetString(options, "ring")
    val options1 = options - "ring"
    var f1 = new DefaultPromise[(String, String)]
    send ! ("get", ring, f1, tableName, keyEncode(key), Compact(options1))
    val f2 = f1.map { x =>
      {
        val (code: String, v1: String) = x
        if (code == Codes.NoItem) {
          None
        } else {
          checkCode(code, v1)
          Some(Json(v1))
        }
      }
    }
    f2
  }
  
  private[persist] def resync(key: JsonKey, options: JsonObject = emptyJsonObject):Future[Unit] = {
    val ring = jgetString(options, "ring")
    val options1 = options - "ring"
    var f1 = new DefaultPromise[(String, String)]
    send ! ("resync", ring, f1, tableName, keyEncode(key), Compact(options1))
    val f2 = f1.map { x =>
      {
        val (code: String, v1: String) = x
        checkCode(code, v1)
      }
    }
    f2
  }

  /**
   *
   * Returns a future that can be used to access a sequence of items.
   * If options are not specified it will include the keys of all items
   * in the table.
   *
   * @param options optional json object containing options.
   *    Options can modify what items are returned and what information is
   *    returned for each item.
   *  - '''"reverse"=true''' if true, return keys in reverse order. Default is false.
   *  - '''"get="kvcde"''' if specified this method returns an object with requested fields
   *      (Key, Value, vector Clock, Deleted, Expires time).
   *  - '''"low"=key''' the lowest key that should be returned.
   *  - '''"includelow"=true''' if true, the low option is inclusive.
   *  If false, the low option is exclusive. Default is false.
   *  - '''"high"=key''' the highest key that should be returned.
   *  - '''"includehigh"=true''' if true, the high option is inclusive.
   *  If false, the high option is exclusive. Default is false.
   *  - '''"prefix"=key''' only keys for which this is a prefix are returned.
   *  - '''"includeprefix"=true''' if true, any key equal to the prefix is included.
   *  If false, any key equal to the prefix is not included. Default is false.
   *  - '''"parent"=key''' only keys which are direct children of this key are included.
   *  This option permits trees (where keys are arrays that represents paths) to be traversed.
   *  - '''"includeparent"=true''' if true, any key equal to the parent is also included.
   *  If false, any key equal to the parent is not included. Default is false.
   *  - '''"prefixtab"="prefixName"''' get items from this prefix table rather than the main table.
   *
   *  @return a future for the first item (if any).
   *  The value of that future upon completion will be either
   *  - None. there is no first item.
   *  - Some(NextItem). the first item.
   *
   */
  def all(options: JsonObject = emptyJsonObject): Future[Option[NextItem]] = {
    // TODO when parent is set, low and high need special treatment
    val ring = jgetString(options, "ring")
    var options1 = JsonObject()

    val isReverse = jgetBoolean(options, "reverse")

    val rincludeLow = jgetBoolean(options, "includelow")
    val rincludeHigh = jgetBoolean(options, "includehigh")
    val rlow = jget(options, "low") match {
      case null => ""
      case j: Json => keyEncode(j)
    }
    val rhigh = jget(options, "high") match {
      case null => "~"
      case j: Json => keyEncode(j)
    }

    val (plow, phigh, pincludeLow, pincludeHigh) = jget(options, "prefix") match {
      case null => ("", "~", false, false)
      case j: Json => {
        val k = keyEncode(j)
        val includePrefix = jgetBoolean(options, "includeprefix")
        (k, k + "\uFFFF", includePrefix, false)
      }
    }

    val (tlow, thigh, tincludeLow, tincludeHigh, parent) = jget(options, "parent") match {
      case null => ("", "~", false, false, null)
      case j: Json => {
        val k = keyEncode(j)
        // TODO must be and array
        val includeParent = jgetBoolean(options, "includeparent")
        (k, k + "\uFFFF", includeParent, false, j)
      }
    }

    val (low1, includeLow1) = {
      if (rlow == plow) {
        (rlow, rincludeLow && pincludeLow)
      } else if (rlow < plow) {
        (plow, pincludeLow)
      } else {
        (rlow, rincludeLow)
      }
    }

    val (high1, includeHigh1) = {
      if (rhigh == phigh) {
        (rhigh, rincludeHigh && pincludeHigh)
      } else if (rhigh > phigh) {
        (phigh, pincludeHigh)
      } else {
        (rhigh, rincludeHigh)
      }
    }

    val (low, includeLow) = {
      if (low1 == tlow) {
        (low1, includeLow1 && tincludeLow)
      } else if (low1 < tlow) {
        (tlow, tincludeLow)
      } else {
        (low1, includeLow1)
      }
    }

    val (high, includeHigh) = {
      if (high1 == thigh) {
        (high1, includeHigh1 && tincludeHigh)
      } else if (thigh > high1) {
        (high1, includeHigh1)
      } else {
        (thigh, tincludeHigh)
      }
    }

    jgetString(options, "get") match {
      case "" =>
      case s => options1 += ("get" -> s)
    }

    jgetString(options, "prefixtab") match {
      case "" =>
      case s => options1 += ("prefixtab" -> s)
    }

    val ni = if (isReverse) {
      new BackwardNextItem(ring, null, high,
        tableName, includeHigh, Compact(options1), low, includeLow, parent,
        send, system)
    } else {
      new ForwardNextItem(ring, null, low,
        tableName, includeLow, Compact(options1), high, includeHigh, parent,
        send, system)
    }
    ni.next()
  }

}