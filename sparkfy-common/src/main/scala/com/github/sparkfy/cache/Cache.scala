package com.github.sparkfy.cache

import collection.mutable

/**
 * Cache interface, implementation should guarantee thread safety
 */
trait Cache[K, V] {

  /**
   * Get value for key, if can't find throw CacheException
   * @param key key
   * @return value for key
   * @throws CacheException if can't find value for key, both in cache and via the provided function
   */
  def get(key: K): V

  /**
   * Get values for keys, if can't find value(s) for some key(s) throw CacheException
   * @param keys keys
   * @param map map for keys can be found in cache or via provided function
   * @return map of keys and values
   * @throws CacheException if can't find value(s) for some key(s), both in cache and via the provided function
   */
  def getAll(keys: Iterable[K], map: Option[mutable.Map[K, V]] = None): Map[K, V]

}
