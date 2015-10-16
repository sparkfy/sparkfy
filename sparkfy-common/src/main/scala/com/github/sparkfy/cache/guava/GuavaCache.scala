package com.github.sparkfy.cache.guava

import com.github.sparkfy.cache.Cache
import com.google.common.cache.LoadingCache

/**
 * Created by yellowhuang on 2015/10/16.
 */
class GuavaCache[K, V](val name: String, private val guavaCache: LoadingCache[K, V]) extends Cache[K, V] {

  override def get(key: K): V = {
    //    guavaCache.get(key)
    null.asInstanceOf[V]
  }


  override def getAll(keys: Iterable[K]): Map[K, V] = {
    null
    //    guavaCache.getAll(keys.asJava).asScala.toMap
  }

}
