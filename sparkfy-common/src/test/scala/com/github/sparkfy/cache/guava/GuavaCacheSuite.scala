package com.github.sparkfy.cache.guava

/**
 * Created by huangyu on 15/10/17.
 */
object GuavaCacheSuite {


  def test(): Unit = {

    val factory = new GuavaCacheFactory(Map[String, String]())
    val cache = factory.createAll[String, String]("test", key => key + "_1", keys => keys.map(k => (k, k + "_2")).toMap)
    println(cache.get("dfd"))
    println(cache.getAll(List("k1", "k3")))

  }

  def main(args: Array[String]): Unit = {
    test()
  }


}
