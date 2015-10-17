package com.github.sparkfy.cache.guava

import com.github.sparkfy.cache.{Cache, CacheFactory}

/**
 * Created by yellowhuang on 2015/10/16.
 */
class GuavaCacheFactory(val conf: Map[String, String]) extends CacheFactory {

  //  override def createBuilder[K, V](name: String): CacheBuilder[K, V] = {
  //    new GuavaCacheBuilder[K, V](name, conf)
  override def createAll[K, V](name: String, get: K => V, getAll: Iterable[K] => Map[K, V]): Cache[K, V] = {
    new GuavaCacheBuilder[K, V](name, conf).build(get, getAll)
  }

  override def create[K, V](name: String, get: K => V): Cache[K, V] = {
    new GuavaCacheBuilder[K, V](name, conf).build(get)
  }
}
