package com.github.sparkfy

/**
 * Created by huangyu on 15/10/11.
 */
class SparkfyException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}
