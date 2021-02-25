package com.abhinab.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFunctions {

  val emps = List(("E101","name1",9000),
    ("E102","name2",9900),
    ("E103","name3",8000),
    ("E104","name4",9100),
    ("E105","name5",9200),
    ("E106","name6",9300),
    ("E107","name7",9400),
    ("E108","name8",9500),
    ("E109","name9",9000),
    ("E110","name10",9000))//.toDF("empId","empName","sal")
  val deps = List(("D101","HR","E101"),
    ("D101","HR","E102"),
    ("D102","Admin","E103"),
    ("D102","Admin","E104"),
    ("D102","Admin","E105"),
    ("D103","IT","E106"),
    ("D103","IT","E107"),
    ("D103","IT","E108"),
    ("D101","HR","E109"),
    ("D101","HR","E1010"))//.toDF("depId","depName","empId")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("StructuredNetworkWordCount").master("local[*]").getOrCreate()
    import spark.implicits._

    val emp = emps.toDF("empId","empName","sal")
    val dep = deps.toDF("depId","depName","empId")

    val analyticWindow = Window.partitionBy("depName").orderBy("sal")
    val secondAnalyticWindow = Window.partitionBy("depName").orderBy('sal asc)
    val aggregateWindow = Window.partitionBy("depName")

    //window aggregate function
    val maxSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("maxSal", max("sal").over(aggregateWindow)).select('depName, 'maxSal, $"emp.*")
    val minSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("minSal", min("sal").over(aggregateWindow)).select('depName, 'minSal, $"emp.*")
    val avgSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("avgSal", avg("sal").over(aggregateWindow)).select('depName, 'avgSal, $"emp.*")
    val countEmployeeByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("numberOfEmp", count("sal").over(aggregateWindow)).select('depName, 'numberOfEmp, $"emp.*")
    val sumSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("sumSal", sum("sal").over(aggregateWindow)).select('depName, 'sumSal, $"emp.*")

    //window ranking function
    val rankSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("rank", rank.over(analyticWindow)).select('depName, 'rank, $"emp.*")
    val denseRankSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("denseRank", dense_rank().over(analyticWindow)).select('depName, 'denseRank, $"emp.*")
    val rowNumberSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("rowNumber", row_number().over(analyticWindow)).select('depName, 'rowNumber, $"emp.*")
    val ntileEmployeeByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("ntile", ntile(2).over(analyticWindow)).select('depName, 'ntile, $"emp.*")
    val percentRankalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("percentRank", percent_rank().over(analyticWindow)).select('depName, 'percentRank, $"emp.*")

    //window analytic Function
    val cumeDistSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("cumDist", cume_dist().over(analyticWindow)).select('depName, 'cumDist, $"emp.*")
    val leadSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("lag", lag('sal, 1).over(analyticWindow)).select('depName, 'lag, $"emp.*")
    val lagSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("lead", lead('sal, 2).over(analyticWindow)).select('depName, 'lead, $"emp.*")

    //secondHighest Sal
    val secondMaxSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("rank", rank.over(analyticWindow)).filter('rank === 2).select('depName, 'rank, $"emp.*")
    val secondMinSalaryByDep = emp.as("emp").join(dep.as("dep"), $"emp.empId" === $"dep.empId").withColumn("rank", rank.over(secondAnalyticWindow)).filter('rank === 2).select('depName, 'rank, $"emp.*")

    //Top two most popular show saved in a dataset
    val data = Seq(("NetFlix","IND","GOT","V001"), ("NetFlix","IND","FRIENDS","V001"), ("NetFlix","IND","BBT","V001"), ("NetFlix","IND","FRIENDS","V002"), ("NetFlix","IND","FRIENDS","V003"), ("NetFlix","IND","FRIENDS","V005"), ("NetFlix","IND","GOT","V002"), ("NetFlix","IND","GOT","V003"), ("NetFlix","IND","GOT","V004"), ("NetFlix","IND","GOT","V005"), ("NetFlix","USA","GOT","USV001"), ("NetFlix","USA","FRIENDS","USV001"), ("NetFlix","USA","BBT","USV001"), ("NetFlix","USA","FRIENDS","USV002"), ("NetFlix","USA","FRIENDS","USV003"), ("NetFlix","USA","FRIENDS","USV005"), ("NetFlix","USA","FRIENDS","USV006"), ("NetFlix","USA","FRIENDS","USV007"), ("NetFlix","USA","FRIENDS","USV008"), ("NetFlix","USA","GOT","USV005"), ("NetFlix","UK","GOT","UKV001"), ("NetFlix","UK","FRIENDS","UKV001"), ("NetFlix","UK","BBT","UKV001"), ("NetFlix","UK","FRIENDS","UKV002"), ("NetFlix","UK","FRIENDS","UKV003"), ("NetFlix","UK","FRIENDS","UKV005"), ("NetFlix","UK","BBT","UKV002"), ("NetFlix","UK","BBT","UKV003"), ("NetFlix","UK","BBT","UKV004"), ("NetFlix","UK","BBT","UKV005"))
    val TVShowDF = data.toDF("serviceProvider","country","show","viewer")
    val top2PopularShowWindow = Window.partitionBy('country, 'show)
    val popularShowDF = TVShowDF.withColumn("numberOfViewer", count('viewer).over(top2PopularShowWindow)).withColumn("DR", dense_rank().over(Window.partitionBy('country).orderBy('numberOfViewer.desc))).filter('DR <= 2)
    popularShowDF.select("serviceProvider","country","show","numberOfViewer","DR").distinct().show(false)
    println(popularShowDF.rdd.toDebugString)

    /*
    +---------------+-------+-------+--------------+---+
    |serviceProvider|country|show   |numberOfViewer|DR |
    +---------------+-------+-------+--------------+---+
    |NetFlix        |USA    |FRIENDS|7             |1  |
    |NetFlix        |USA    |GOT    |2             |2  |
    |NetFlix        |UK     |BBT    |5             |1  |
    |NetFlix        |UK     |FRIENDS|4             |2  |
    |NetFlix        |IND    |GOT    |5             |1  |
    |NetFlix        |IND    |FRIENDS|4             |2  |
    +---------------+-------+-------+--------------+---+
     */

  }
}
case class Salary(depName: String, empNo: Long, salary: Long)