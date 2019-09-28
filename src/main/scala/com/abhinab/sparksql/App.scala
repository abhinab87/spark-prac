package com.abhinab.sparksql

import java.util.StringTokenizer

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

/**
  * Created by abhin on 9/15/2017.
  */
object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("StructuredNetworkWordCount").master("local[*]").getOrCreate()
    import spark.implicits._
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    val line = "C:/Users/abhin/Desktop/PersonalInfo.txt"
    val st = new StringTokenizer(line,",")
    val per = sc.textFile("C:/Users/abhin/Desktop/PersonalInfo.txt").map(_.split(", "))
    val personalInfo = sc.textFile("C:/Users/abhin/Desktop/PersonalInfo.txt").map(_.split(", ")).map(x => PersonalInfo(x(0),x(1),x(2),x(3).trim.toInt)).toDF()
    val studentInfo = sc.textFile("C:/Users/abhin/Desktop/StudentInfo.txt").map(_.split(", ")).map(x => StudentInfo(x(0).trim.toInt,x(1),x(2),x(3),x(4))).toDF()
    val studentInfo1 = sc.textFile("C:/Users/abhin/Desktop/StudentInfo.txt").map(_.split(", ")).map(x => StudentInfo(x(0).trim.toInt,x(1),x(2),x(3),x(4))).toDF()
    val markInfo = sc.textFile("C:/Users/abhin/Desktop/MarkInfo.txt").map(_.split(", ")).map(x => MarkInfo(x(0).trim.toInt,x(1).trim.toInt,x(2).trim.toInt)).toDF()
    val personalInfo1 = sc.textFile("C:/Users/abhin/Desktop/sparkFile/Per*").map(_.split(",")).map(x => PersonalInfo1(x(0).trim.toInt,x(1),x(2),x(3),x(4).trim.toInt)).toDF()
    val schoolInfo = personalInfo.join(studentInfo,personalInfo("fName") === studentInfo("fName")).orderBy(studentInfo("id")).select(studentInfo("id"),studentInfo("fName"), (studentInfo("lName")),studentInfo("schoolName"))
    val schoolgrp = studentInfo.join(markInfo,studentInfo("id") === markInfo("id")).groupBy(schoolInfo("schoolName")).avg("SM").show()
    val percentCalxc = studentInfo.join(markInfo,studentInfo("id") === markInfo("id")).select(studentInfo("id"),studentInfo("fName"), (studentInfo("lName")),studentInfo("schoolName"),percentCal(markInfo("FM"),markInfo("SM")) as "Percent")
    //percentCalxc.show()
    //schoolInfo.show()
    personalInfo1.show
    personalInfo1.registerTempTable("personalInfo1")
    studentInfo.registerTempTable("studentInfo")
    val x2 = new StructType()
    //val csudf = new CollectSetFunction(x2.getClass)
    val x = sc.textFile("C:/Users/abhin/Desktop/sparkFile/Per*").map(_.split(","))
    val y = x.groupBy(x => x(3)).map(x => (x._1,x._2.map(x=>x(4).trim.toInt))).map(x => (x._1,x._2.max))
    y.collect().foreach(println(_))
    //sqlContext.read.

    //import sqlContext.implicits._
    val customers = sc.parallelize(List(
      ("Alice", "2016-05-01", 50.00),
      ("Alice", "2016-05-03", 45.00),
      ("Alice", "2016-05-04", 55.00),
      ("Bob", "2016-05-01", 25.00),
      ("Bob", "2016-05-04", 29.00),
      ("Bob", "2016-05-06", 27.00))).
      toDF("name", "date", "amountSpent")

    val col1 = customers.groupBy("name").agg(collect_list("amountSpent").alias("x0x"))

    // Create a window spec.
    val wSpec1 = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)

    // Calculate the moving average
    customers.withColumn("movingAvg",
      avg(customers("amountSpent")).over(wSpec1)).show()

    val wSpec2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue, 0)

    // Create a new column which calculates the sum over the defined window frame.
    customers.withColumn("cumSum",
      sum(customers("amountSpent")).over(wSpec2)).show()

    // Window spec. No need to specify a frame in this case.
    val wSpec3 = Window.partitionBy("name").orderBy("date")

    // Use the lag function to look backwards by one row.
    customers.withColumn("prevAmountSpent",
      lag(customers("amountSpent"), 1).over(wSpec3)).show()

    customers.withColumn("rank", rank().over(wSpec3)).show()
    customers.withColumn("row_num",row_number() over(wSpec3)).show()
    val ar = sqlContext.sql("select id, struct(fName,lName) as name from personalInfo1")
    ar.show
    println(ar.schema)
    /*val col = ar.groupBy('id).agg(withAggregateFunction(CollectList(ar("name").expr)).alias("name"))
    col.show(false)
    col.printSchema()
    val df1 = studentInfo.union(studentInfo1)
    val df = personalInfo1.join(studentInfo,personalInfo1("id") === studentInfo("id"),"left_outer")
      df1.show()
    val makeName = udf((firstName: String, lastName: String) => Name(firstName, lastName))
    val stud = studentInfo.withColumn("name",makeName(studentInfo("fName"),studentInfo("lName"))).withColumn("name1",makeName(studentInfo("schoolName"),studentInfo("cls")))//.select(studentInfo("id"),studentInfo("schoolName"),studentInfo("cls"))
    stud.printSchema()
    stud.show(false)
    //studentInfo.show(false)
    println(studentInfo.count+"++++++++++++++++++++++++++++"+personalInfo1.count()+"++++++++++++++++++++++++++++"+df1.count)
    col1.show()
    col1.printSchema()*/
  }
  def percentCal(fm:Column, sm:Column) =  sm/fm*100
  private def withAggregateFunction(func: AggregateFunction, isDistinct: Boolean = false): Column = new Column(func.toAggregateExpression(isDistinct))

}
case class Name(firstName:String,lastName:String)