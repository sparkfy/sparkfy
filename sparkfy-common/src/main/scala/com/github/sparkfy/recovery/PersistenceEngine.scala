package com.github.sparkfy.recovery

import scala.reflect.ClassTag

/**
 * Allows Master to persist any state that is necessary in order to recover from a failure.
 * The implementation of this trait defines how name-object pairs are stored or retrieved.
 */
abstract class PersistenceEngine {

  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   */
  def persist(name: String, obj: Object)

  /**
   * Defines how the object referred by its name is removed from the store.
   */
  def unpersist(name: String)

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   */
  def read[T: ClassTag](prefix: String): Seq[T]

  def close() {}

}

class BlackHolePersistenceEngine extends PersistenceEngine {

  override def persist(name: String, obj: Object): Unit = {}

  override def unpersist(name: String): Unit = {}

  override def read[T: ClassTag](name: String): Seq[T] = Nil

}