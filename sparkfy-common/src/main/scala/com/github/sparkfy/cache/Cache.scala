package com.github.sparkfy.cache

/**
 * Created by yellowhuang on 2015/10/16.
 */
trait Cache[K, V] {

  def get(key: K): V

  def getAll(keys: Iterable[K]): Map[K, V]

}
