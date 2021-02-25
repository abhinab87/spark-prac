package com.abhinab.sparkcore

import com.abhinab.models.Student

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    println("Hello World!")
    val conf = new org.apache.spark.SparkConf().setAppName("Abhinab").setMaster("local[*]")
    val sc = new org.apache.spark.SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val path = "C:/Users/abhin/Desktop/student.txt"
    //val line = sc.textFile(path).map(_.split(", ")).collect().foreach(println(_))
    //val lineinFlatMap = sc.textFile(path).flatMap(_.split(", ")).collect().foreach(println(_))
    val students = sc.textFile(path).map(_.split(", ")).map(x => new Student(x(0).toInt, x(1), x(2), x(3).toDouble, x(4).toInt)) //.toDF
    val stud = sc.textFile(path).map(_.split(", "))
    val x = students
    students.collect().foreach(println(_))
    //println(stud.collect().foreach(x =>  println(x(0)+"::"+x(1))))
    val lst = Iterable(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    lst.flatMap(_.toString)
    println("::::"+ sum(lst))
    //rdd.groupBy(_.charAt(0))
    //students.collect().foreach(println(_))
    val avgMarkPerSchool = students.groupBy(x => x.school).map{ case (x, y) => (x,sum(y.map(_.SM))/y.size) }.foreach(println)
    val path1 = "C:/Users/abhin/Desktop/1.txt"
    //val text = sc.textFile(path1).map(_.split(" ")).collect().foreach(println)
    //val text1 = sc.textFile(path1).flatMap(_.split(" ")).collect().foreach(println)
    //println(text)
    val ar = Array(1,2,7,6)
    ar.sortWith(_ < _)
  }
  def sum(xs: Iterable[Int]): Int = {
    xs match {
      case x :: tail => x + sum(tail) // if there is an element, add it to the sum of the tail
      case Nil => 0 // if there are no elements, then the sum is 0
    }
  }
}