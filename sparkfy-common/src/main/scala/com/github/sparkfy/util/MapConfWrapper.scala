package com.github.sparkfy.util

/**
 * Created by yellowhuang on 2015/10/14.
 */
class MapConfWrapper(val conf: Map[String, String]) {

  private final val avroNamespace = "avro.schema."

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  private[this] def get(key: String): String = {
    conf.get(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    conf.get(key).getOrElse(defaultValue)
  }

  /**
   * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then seconds are assumed.
   * @throws NoSuchElementException
   */
  def getTimeAsSeconds(key: String): Long = {
    Utils.timeStringAsSeconds(get(key))
  }

  /**
   * Get a time parameter as seconds, falling back to a default if not set. If no
   * suffix is provided then seconds are assumed.
   */
  def getTimeAsSeconds(key: String, defaultValue: String): Long = {
    Utils.timeStringAsSeconds(get(key, defaultValue))
  }

  /**
   * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then milliseconds are assumed.
   * @throws NoSuchElementException
   */
  def getTimeAsMs(key: String): Long = {
    Utils.timeStringAsMs(get(key))
  }

  /**
   * Get a time parameter as milliseconds, falling back to a default if not set. If no
   * suffix is provided then milliseconds are assumed.
   */
  def getTimeAsMs(key: String, defaultValue: String): Long = {
    Utils.timeStringAsMs(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then bytes are assumed.
   * @throws NoSuchElementException
   */
  def getSizeAsBytes(key: String): Long = {
    Utils.byteStringAsBytes(get(key))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set. If no
   * suffix is provided then bytes are assumed.
   */
  def getSizeAsBytes(key: String, defaultValue: String): Long = {
    Utils.byteStringAsBytes(get(key, defaultValue))
  }

  /**
   * Get a size parameter as bytes, falling back to a default if not set.
   */
  def getSizeAsBytes(key: String, defaultValue: Long): Long = {
    Utils.byteStringAsBytes(get(key, defaultValue + "B"))
  }

  /**
   * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Kibibytes are assumed.
   * @throws NoSuchElementException
   */
  def getSizeAsKb(key: String): Long = {
    Utils.byteStringAsKb(get(key))
  }

  /**
   * Get a size parameter as Kibibytes, falling back to a default if not set. If no
   * suffix is provided then Kibibytes are assumed.
   */
  def getSizeAsKb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsKb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Mebibytes are assumed.
   * @throws NoSuchElementException
   */
  def getSizeAsMb(key: String): Long = {
    Utils.byteStringAsMb(get(key))
  }

  /**
   * Get a size parameter as Mebibytes, falling back to a default if not set. If no
   * suffix is provided then Mebibytes are assumed.
   */
  def getSizeAsMb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsMb(get(key, defaultValue))
  }

  /**
   * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
   * suffix is provided then Gibibytes are assumed.
   * @throws NoSuchElementException
   */
  def getSizeAsGb(key: String): Long = {
    Utils.byteStringAsGb(get(key))
  }

  /**
   * Get a size parameter as Gibibytes, falling back to a default if not set. If no
   * suffix is provided then Gibibytes are assumed.
   */
  def getSizeAsGb(key: String, defaultValue: String): Long = {
    Utils.byteStringAsGb(get(key, defaultValue))
  }

  /** Gets all the avro schemas in the configuration used in the generic Avro record serializer */
  def getAvroSchema: Map[Long, String] = {
    conf.filter(kv => kv._1.startsWith(avroNamespace)).map(kv => {
      (kv._1.substring(avroNamespace.length).toLong, kv._2)
    })
  }


}

object MapConfWrapper {

  implicit def toMapConfWrapper(map: Map[String, String]) = {
    new MapConfWrapper(map)
  }
}