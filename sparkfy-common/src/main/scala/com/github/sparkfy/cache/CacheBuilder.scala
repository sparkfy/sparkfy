package com.github.sparkfy.cache

/**
 * Created by yellowhuang on 2015/10/16.
 */
trait CacheBuilder[K, V] {

  def build(get: K => V, getAll: Iterable[K] => Map[K, V]): Cache[K, V]

  def build(get: K => V): Cache[K, V]

}
