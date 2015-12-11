package com.github.sparkfy.util

/**
 * Created by yellowhuang on 2015/12/11.
 */
object SparkfyIterableWrapper {

  implicit def toTopNIterable[A](iter: Iterable[A]) = new {
    def top[B](n: Int, f: A => B)(implicit ord: Ordering[B]): List[A] = {
      def updateSofar(sofar: List[A], el: A): List[A] = {

        if (ord.compare(f(el), f(sofar.head)) > 0)
          (el :: sofar.tail).sortBy(f)
        else sofar
      }

      val (sofar, rest) = iter.splitAt(n)
      (sofar.toList.sortBy(f) /: rest)(updateSofar(_, _)).reverse
    }
  }

  def main(args: Array[String]) {
    case class A(s: String, i: Int)
    val li = List(4, 3, 6, 7, 1, 2, 9, 5).map(i => A(i.toString(), i))
    println(li.top(3, _.i))
  }
}
