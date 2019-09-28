package com.abhinab.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by abhin on 4/27/2019.
  */
object SelfJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("StructuredNetworkWordCount").master("local[*]").getOrCreate()
    import spark.implicits._
    val sqlContext = spark.sqlContext

    val customers = spark.createDataFrame(List(
      (101, "Alice", 1001),
      (102, "Alice1", 1002),
      (103, "Alice2", 1003),
      (104, "Bob", 1004),
      (1003, "Bob1", 10001),
      (1004, "Bob2", 10001),
      (1001, "Bob3", 10001),
      (1002, "Bob4", 10001),
      (10001, "CEO", 1))).
      toDF("empId", "empName", "supvrId")
    val customer1 = customers.select($"empId",$"empName")
    val details = customers.as("c1").join(customers.as("c2"), $"c1.supvrId" === $"c2.empid", "left").select($"c1.*",$"c2.empName".as("supvrName"))
    details.orderBy($"empName").show(false)
  }

}
