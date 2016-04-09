package com.github.sparkfy.cache

import scala.collection.mutable

/**
 * Created by yellowhuang on 2016/4/8.
 */
class NoneCacheFactory extends CacheFactory {

  override def createAll[K, V](name: String, get: (K) => V, getAll: (Iterable[K]) => Map[K, V]): Cache[K, V] = new NoneCache(get, getAll)

  override def create[K, V](name: String, get: (K) => V): Cache[K, V] = new NoneCache(get, null)
}

class NoneCache[K, V](_get: (K) => V, _getAll: (Iterable[_ <: K]) => Map[K, V]) extends Cache[K, V] {

  override def get(key: K): V = {
    _get(key)
  }

  override def getAll(keys: Iterable[K], map: Option[mutable.Map[K, V]] = None): Map[K, V] = {
    _getAll(keys)
  }
}