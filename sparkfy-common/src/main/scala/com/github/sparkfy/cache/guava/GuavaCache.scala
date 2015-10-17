package com.github.sparkfy.cache.guava

import java.util
import java.util.{Map => JMap}

import com.github.sparkfy.cache.{CacheException, Cache}
import com.google.common.cache.LoadingCache
import java.lang.{Iterable => JIterable}
import collection.mutable

import scala.collection.JavaConverters._

/**
 * Created by yellowhuang on 2015/10/16.
 */
class GuavaCache[K, V] private[guava](val name: String, private val guavaCache: LoadingCache[Object, Object]) extends Cache[K, V] {

  override def get(key: K): V = {
    try {
      guavaCache.get(key.asInstanceOf[Object]).asInstanceOf[V]
    } catch {
      case e: Throwable =>
        throw new CacheException(s"Can't find value for key:${key}", e)
    }
    //    null.asInstanceOf[V]
  }


  override def getAll(keys: Iterable[K], map: Option[mutable.Map[K, V]] = None): Map[K, V] = {
    try {
      val re = guavaCache.getAll(keys.asJava.asInstanceOf[JIterable[Object]]).asInstanceOf[JMap[K, V]].asScala.toMap
      map.foreach(_ ++= re)
      re
    } catch {
      case e: Throwable =>
        map.foreach(mp => {
          val it = keys.iterator
          while (it.hasNext) {
            try {
              val k = it.next()
              mp.put(k, get(k))
            } catch {
              case _: Throwable =>
            }
          }
        }
        )
        throw new CacheException("Can't find values for some keys", e)
    }
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
