package com.github.sparkfy.cache

/**
 * Created by huangyu on 15/10/17.
 */
class CacheException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}
