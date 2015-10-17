package com.github.sparkfy.cache.guava


import java.lang.{Iterable => JIterable}
import java.util.{Map => JMap}
import java.util.concurrent.TimeUnit

import com.github.sparkfy.cache.{Cache}
import com.google.common.cache.{CacheBuilder => GCacheBuilder, CacheLoader}

import scala.collection.JavaConverters._

/**
 * Created by yellowhuang on 2015/10/16.
 */
class GuavaCacheBuilder[K, V] private[guava](val name: String, conf: Map[String, String]) {
  val builder = GuavaCacheBuilder.createCacheBuilder[K, V](name, conf)

  def build(get: (K) => V, getAll: (Iterable[_ <: K]) => Map[K, V]): Cache[K, V] = {
    val cacheLoader: CacheLoader[Object, Object] = new CacheLoader[Object, Object] {
      override def load(key: Object): Object = get(key.asInstanceOf[K]).asInstanceOf[Object]

      override def loadAll(keys: JIterable[_ <: Object]): JMap[Object, Object] = {
        getAll(keys.asInstanceOf[JIterable[_ <: K]].asScala).asJava.asInstanceOf[JMap[Object, Object]]
      }
    }
    new GuavaCache[K, V](name, builder.build(cacheLoader))
  }

  def build(get: (K) => V): Cache[K, V] = {

    new GuavaCache[K, V](name, builder.build(new CacheLoader[Object, Object] {
      override def load(key: Object): Object = get(key.asInstanceOf[K]).asInstanceOf[Object]
    }))
  }
}

object GuavaCacheBuilder {
  def createCacheBuilder[K, V](name: String, conf: Map[String, String]): GCacheBuilder[Object, Object] = {
    val b = GCacheBuilder.newBuilder()
    conf.get(s"cache.${name}.guava.maxSize").foreach(s => b.maximumSize(s.toInt))
    conf.get(s"cache.${name}.guava.expireAfterWrite").foreach(s => b.expireAfterWrite(s.toLong, TimeUnit.MINUTES))
    conf.get(s"cache.${name}.guava.expireAfterAccess").foreach(s => b.expireAfterAccess(s.toLong, TimeUnit.MINUTES))
    b
  }
}


