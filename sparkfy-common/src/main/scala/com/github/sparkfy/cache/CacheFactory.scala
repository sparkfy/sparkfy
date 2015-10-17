package com.github.sparkfy.cache

import com.github.sparkfy.util.Utils

/**
 * Created by yellowhuang on 2015/10/16.
 */
trait CacheFactory {

  def createAll[K, V](name: String, get: K => V, getAll: Iterable[K] => Map[K, V]): Cache[K, V]

  def create[K, V](name: String, get: K => V): Cache[K, V]

}

object CacheFactory {

  val cacheNames = Map(
    "guava" -> "com.github.sparkfy.cache.guava.GuavaCacheFactory")

  def getCacheFactory(conf: Map[String, String]): CacheFactory = {

    val cacheName = conf.getOrElse("cache", "guava")
    val cacheFactoryClassName = cacheNames.getOrElse(cacheName.toLowerCase, cacheName)
    Utils.classForName(cacheFactoryClassName).getConstructor(classOf[Map[String, String]]).newInstance(conf).asInstanceOf[CacheFactory]

  }

  def getCacheFactory(name: String, conf: Map[String, String]): CacheFactory = {
    val cacheFactoryClassName = cacheNames.getOrElse(name.toLowerCase, name)
    Utils.classForName(cacheFactoryClassName).getConstructor(classOf[Map[String, String]]).newInstance(conf).asInstanceOf[CacheFactory]
  }
}