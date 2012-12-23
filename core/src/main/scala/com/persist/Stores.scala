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
import ExceptionOps._
import akka.actor.ActorContext

/**
 * This object contains the traits that must be implemented to add
 * a new local store. An OStore local store contain both control information
 * and data. Unless you are implementing a new local store there is no
 * need to understand these traits.
 * 
 * The package com.persist.store contains built-in implementations of these traits.
 */
object Stores {

  private[persist] val NOVAL = ""

  private[persist] object Store {
    def apply(name: String, config: Json, context: ActorContext, create: Boolean): Store = {
      try {
        val className = jgetString(config, "class")
        val c = Class.forName(className)
        val obj = c.newInstance()
        val store = obj.asInstanceOf[Store]
        store.init(name, config, context, create)
        store
      } catch {
        case x => {
          val ex = InternalException(x.toString())
          throw ex
        }
      }
    }
  }

  /**
   * The trait for local stores.
   * There is a local store for
   *  - Each server (containing server control information including configurations for each database on the server)
   *  - Each node of a database (contains table items and table control information)
   *
   * Each Store contains a set of named StoreTables.
   *
   */
  trait Store {

    /**
     * This method is called by the system to initial the store before any other
     * method calls. A new empty store is created if it does not already exist.
     *
     * @param name the name of the store.
     * @param context if the Store uses actors they are created in this context.
     * @param create if true, a new empty store is created, even if one already exists.
     */
    def init(name: String, config: Json, context: ActorContext, create: Boolean)

    /**
     * True if a new empty store was created.
     */
    def created: Boolean

    /**
     * Gets a store table.
     * If called multiple times with the same tableName, it
     * will return the same StoreTable.
     *
     * @param tableName the name of the StoreTable
     * @return the StoreTable
     */
    def getTable(tableName: String): StoreTable

    /**
     * Deleted a store table.
     *
     * @param tableName the name of the StoreTable
     */
    def deleteTable(tableName: String)

    /**
     * Closes the store. It is called last. After it is called
     * no other methods will be called.
     */
    def close()
  }

  /**
   * A local store table contains OStore items and control information.
   * Each local store table has three key-value parts
   *  - meta. Per item metadata. Keys here are ordered.
   *  - vals. Per item values. Keys here need not be ordered,
   *  - control. System control information. Keys here need not be ordered.
   *
   * Keys and values are each strings.
   *
   * For each OStore item, its key is used to look up its metadata in meta
   * and its value in vals. If the item is a tombstone there will be a meta value
   * but no vals value.
   */
  trait StoreTable {

    /**
     * The encosing store.
     */
    val store: Store

    /**
     * The StoreTable name.
     */
    val tableName: String

    /**
     * The number of key-values in meta. This will be
     * the number of OStore items in the table (including tombstones)
     *
     * @return the size of meta.
     */
    def size(): Long

    /**
     * The number of key-values in vals. This will be
     * the number of OStore items in the table (excluding tombstones)
     *
     * @return the size of vals.
     */
    def vsize(): Long

    /**
     * Get the first key in meta.
     *
     * @return the first key in meta if it exists.
     */
    def first(): Option[String]

    /**
     * Get the last key in meta.
     *
     * @return the last key in meta if it exists.
     */
    def last(): Option[String]

    /**
     * Get the next key in meta.
     *
     * @param key the key.
     * @param equal if true include the key itself.
     * @return the next key in meta that is >= (equal is true) or > (equal is false) if it exists.
     */
    def next(key: String, equal: Boolean): Option[String]

    /**
     * Get the previous key in meta.
     *
     * @param key the key.
     * @param equal if true include the key itself.
     * @return the previous key in meta that is <= (equal is true) or < (equal is false) if it exists.
     */
    def prev(key: String, equal: Boolean): Option[String]

    /**
     * Get a meta value.
     *
     * @param key the key.
     * @return the value of the key in meta if it exists
     */
    def getMeta(key: String): Option[String]

    /**
     * Get a vals value.
     *
     * @param key the key.
     * @return the value of the key in vals if it exists
     */
    def get(key: String): Option[String]

    /** 
     * Puts values into meta and vals.
     * 
     * @param key the key.
     * @param metav the value to be put in meta.
     * @param value the value to be put in vals. If None, any value in vals is removed.
     * @param fast if true the commit of the value to disk may be delayed and batched.
     */
    def put(key: String, metav: String, value: Option[String], fast: Boolean)

    private[persist] def put(key: String, metav: String, value: String, fast: Boolean = false) {
      if (value == NOVAL) {
        put(key, metav, None, fast)
      } else {
        put(key, metav, Some(value), fast)
      }
    }

    /**
     * Removes a key from meta and vals.
     * 
     * @param key the key.
     */
    def remove(key: String)

    /** 
     * Closes the store table. This is called last and no other methods 
     * are called after it.
     */
    def close()

    /**
     * Put a new value in control.
     * 
     * @param key the key.
     * @param value the value
     */
    def putControl(key: String, value: String)

    /**
     * Get a control value.
     *
     * @param key the key.
     * @return the value of the key in control if it exists
     */
    def getControl(key: String): Option[String]

    /**
     * Removes a key from control.
     * 
     * @param key the key.
     */
    def removeControl(key: String)
  }

}