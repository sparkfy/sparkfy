package com.github.sparkfy.cache

/**
 * Created by yellowhuang on 2015/10/16.
 */
trait CacheFactory {

  def createBuilder[K, V](name:String): CacheBuilder[K, V]

}
