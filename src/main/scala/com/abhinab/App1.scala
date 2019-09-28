package com.abhinab

import scala.annotation.tailrec
import scala.io.Source

object App1 {
  val squareSum = (x: Int, y: Int) => (x*x + y*y)
  val cubeSum = (x: Int, y: Int) => (x*x*x + y*y*y)
  val intSum = (x: Int, y: Int) => (x + y)
  val squaredSum = addition(squareSum, 1, 2)
  val cubedSum = addition(cubeSum, 1, 2)
  val normalSum = addition(intSum, 1, 2)

  def main(args: Array[String]): Unit = {
    val files = Source.fromFile("src/main/resources/Student.csv").getLines().toList.map(_.split(",")).groupBy(x => x(2))
      .map(x => (x._1, x._2.map(x => x(4)).sortWith(_ > _))).map(x => (x._1, if(x._2.length == 1)x._2(0) else x._2(1)))
    println(files)

    var pList = List(1, 2, 3, 4)
    val list = List.range(1, 100)
    println(sum(list))
    println(sum2(list))
    println(sum3(list))
    println(sumWithReduce(list))
    println(product2(pList))
    println(max(pList))
    //println(fib(1,2))
    println(fib(9))
    pList = pList :+ 10
    println(pList)
    println(squaredSum)
  }

  def addition(f: (Int, Int) => Int,a: Int, b:Int): Int = f(a,b)

  def factorial2(n: Long): Long = {
    @tailrec
    def factorialAccumulator(acc: Long, n: Long): Long = {
      if (n == 0) acc
      else factorialAccumulator(n*acc, n-1)
    }
    factorialAccumulator(1, n)
  }

  def fib(x: Int): BigInt = {
    @tailrec
    def fibHelper(x: Int, prev: BigInt = 0, next: BigInt = 1): BigInt = x match {
      case 0 => prev
      case 1 => next
      case _ => fibHelper(x - 1, next, (next + prev))
    }
    fibHelper(x)
  }


  def fib(prevPrev: Int, prev: Int) {
    val next = prevPrev + prev
    println(next)
    if (next > 1000000) System.exit(0)
    fib(prev, next)
  }

  // (1) yields a "java.lang.StackOverflowError" with large lists
  def sum(ints: List[Int]): Int = ints match {
    case Nil => 0
    case x :: tail => x + sum(tail)
  }

  // (2) tail-recursive solution
  def sum2(ints: List[Int]): Int = {
    @tailrec
    def sumAccumulator(ints: List[Int], accum: Int): Int = {
      ints match {
        case Nil => accum
        case x :: tail => sumAccumulator(tail, accum + x)
      }
    }
    sumAccumulator(ints, 0)
  }

  // (3) good descriptions of recursion here:
  // stackoverflow.com/questions/12496959/summing-values-in-a-list
  // this example is from that page:
  def sum3(xs: List[Int]): Int = {
    if (xs.isEmpty) 0
    else xs.head + sum3(xs.tail)
  }

  def sumWithReduce(ints: List[Int]) = {
    ints.reduceLeft(_ + _)
  }

  def product2(ints:List[Int]):Int={
    @tailrec
    def productWithAccum(ints:List[Int],accum:Int):Int= ints match {
      case x :: tail =>productWithAccum(tail, accum*x)
      case Nil =>accum
    }

    productWithAccum(ints,1)
  }

  def max(ints:List[Int]):Int={
    @tailrec
    def maxNumber(ints:List[Int], accum:Int):Int= ints match {
      case x :: tail => maxNumber(tail, if(x > accum) x else accum)
      case Nil => accum
    }
    maxNumber(ints,0)
  }

}
