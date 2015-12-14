package com.github.sparkfy.cache.guava

import com.github.sparkfy.SparkfyFunSuite
import com.github.sparkfy.cache.CacheFactory

/**
 * Created by huangyu on 15/10/17.
 */
class GuavaCacheSuite extends SparkfyFunSuite {


  test("Guava cache") {

    //    val factory = new GuavaCacheFactory(Map[String, String]())
    val factory = CacheFactory.getCacheFactory(Map[String, String]())
    val cache = factory.createAll[String, String]("test", key => key + "_1", keys => keys.map(k => (k, k + "_2")).toMap)
    assert(cache.get("dfd") == "dfd_1")
    cache.getAll(List("k1", "k3")).foreach(kv => assert(kv._2 == kv._1 + "_2"))

  }


}
