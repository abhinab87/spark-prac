package com.abhinab.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("StructuredNetworkWordCount").master("local[*]").getOrCreate()
    import spark.implicits._

    val emp = List(("E101","name1",9000),
      ("E102","name2",9900),
      ("E103","name3",8000),
      ("E104","name4",9100),
      ("E105","name5",9200),
      ("E106","name6",9300),
      ("E107","name7",9400),
      ("E108","name8",9500),
      ("E109","name9",9000),
      ("E110","name10",9000)).toDF("empId","empName","sal")
    val dep = List(("D101","HR","E101"),
      ("D101","HR","E102"),
      ("D102","Admin","E103"),
      ("D102","Admin","E104"),
      ("D102","Admin","E105"),
      ("D103","IT","E106"),
      ("D103","IT","E107"),
      ("D103","IT","E108"),
      ("D101","HR","E109"),
      ("D101","HR","E1010")).toDF("depId","depName","empId")

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

  }
}
case class Salary(depName: String, empNo: Long, salary: Long)