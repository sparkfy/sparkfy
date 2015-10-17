package com.github.sparkfy.cache

/**
 * Created by yellowhuang on 2015/10/16.
 */
trait CacheFactory {

  def createAll[K, V](name: String, get: K => V, getAll: Iterable[K] => Map[K, V]): Cache[K, V]

  def create[K, V](name: String, get: K => V): Cache[K, V]

}
