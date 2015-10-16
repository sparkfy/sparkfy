package com.github.sparkfy.cache.guava

import com.github.sparkfy.cache.{CacheBuilder, CacheFactory}

/**
 * Created by yellowhuang on 2015/10/16.
 */
class GuavaCacheFactory(val conf: Map[String, String]) extends CacheFactory {

  override def createBuilder[K, V](name: String): CacheBuilder[K, V] = {
    null
    //    new GuavaCacheBuilder[K, V](name, conf)
  }

}
