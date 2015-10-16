package com.github.sparkfy.cache.guava


import java.lang.{Iterable => JIterable}
import java.util.concurrent.TimeUnit

import com.github.sparkfy.cache.{Cache, CacheBuilder}
import com.google.common.cache.{CacheBuilder => GCacheBuilder, CacheLoader}

/**
 * Created by yellowhuang on 2015/10/16.
 */
class GuavaCacheBuilder[K, V](val name: String, conf: Map[String, String]) extends CacheBuilder[K, V] {
  val builder = GuavaCacheBuilder.createCacheBuilder[K,V](name, conf)

  override def build(get: (K) => V, getAll: (Iterable[_ <: K]) => Map[K, V]): Cache[K, V] = {
    val cacheLoader: CacheLoader[K, V] = new CacheLoader[K, V] {
      override def load(key: K): V = get(key)
    }
    new GuavaCache[K, V](name, builder.build(cacheLoader))
  }

  override def build(get: (K) => V): Cache[K, V] = {
    val cacheLoader: CacheLoader[K, V] = new CacheLoader[K, V] {
      override def load(key: K): V = get(key)
    }
    new GuavaCache[K, V](name, builder.build(cacheLoader))
  }
}

object GuavaCacheBuilder {
  def createCacheBuilder[K,V](name: String, conf: Map[String, String]): GCacheBuilder[_, _] = {
    val b = GCacheBuilder.newBuilder()
    conf.get(s"cache.${name}.guava.maxSize").foreach(s => b.maximumSize(s.toInt))
    conf.get(s"cache.${name}.guava.expireAfterWrite").foreach(s => b.expireAfterWrite(s.toLong, TimeUnit.MINUTES))
    conf.get(s"cache.${name}.guava.expireAfterAccess").foreach(s => b.expireAfterAccess(s.toLong, TimeUnit.MINUTES))
    b
  }
}


