package com.github.sparkfy.cache.guava

import java.util
import java.util.{Map => JMap}

import com.github.sparkfy.cache.Cache
import com.google.common.cache.LoadingCache
import java.lang.{Iterable => JIterable}

import scala.collection.JavaConverters._

/**
 * Created by yellowhuang on 2015/10/16.
 */
class GuavaCache[K, V] private[guava](val name: String, private val guavaCache: LoadingCache[Object, Object]) extends Cache[K, V] {

  override def get(key: K): V = {
    guavaCache.get(key.asInstanceOf[Object]).asInstanceOf[V]
    //    null.asInstanceOf[V]
  }


  override def getAll(keys: Iterable[K]): Map[K, V] = {
    guavaCache.getAll(keys.asJava.asInstanceOf[JIterable[Object]]).asInstanceOf[JMap[K, V]].asScala.toMap
  }

}

object GuavaCache {
  def main(args: Array[String]): Unit = {
    val jl = new util.LinkedList[String]()
    jl.add("dfd")
    jl.asInstanceOf[util.LinkedList[Object]]
    println(jl.get(0))

  }

}
