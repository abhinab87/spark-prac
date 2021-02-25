package com.abhinab.sparksql

import com.sun.xml.internal.bind.v2.TODO
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables._

object DeltaFunctionImplementation {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder.appName("delta_lake").master("local[*]").config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1").getOrCreate()
      import spark.implicits._

    val empdf = WindowFunctions.emps.toDF("empId","empName","sal")
    val depdf = WindowFunctions.deps.toDF("depId","depName","empId")

    val employee = empdf.join(depdf, empdf("empId") === depdf("empId")).select(empdf("empId"), empdf("empName"), empdf("sal"), depdf("depId"), depdf("depName"))

    employee.write.format("delta").save("/tmp/delta-table")

    val df = spark.read.format("delta").load("/tmp/delta-table")
    df.show(false)

    val data = List(("E110","name10",10000,"D103","IT")).toDF("empId","empName","sal","depId","depName")
    data.write.format("delta").mode("overwrite").save("/tmp/delta-table")
    df.show()

    val deltaTable = DeltaTable.forPath("/tmp/delta-table")


    // Update every even value by adding 100 to it
    deltaTable.update(
      condition = 'depId === "D103",
      set = Map("sal" -> expr("sal + 100000")))



    // Delete every even value
    //deltaTable.delete(condition = expr("id % 2 == 0"))



    // Upsert (merge) new data
    val newData = List(("E111","name11",10000,"D103","IT"),
      ("E112","name12",10000,"D103","IT"),
      ("E113","name13",10000,"D103","IT"),
      ("E114","name14",10000,"D103","IT"),
      ("E115","name15",10000,"D103","IT")).toDF("empId","empName","sal","depId","depName")

    deltaTable.as("oldData")
      .merge(
        newData.as("newData"),
        "oldData.empId = newData.empId")
      .whenMatched
      .update(Map("empId" -> col("newData.empId"),"empName" -> col("newData.empName"),"sal" -> col("newData.sal"),"depId" -> col("newData.depId"),"depName" -> col("newData.depName")))
      .whenNotMatched
      .insert(Map("empId" -> col("newData.empId"),"empName" -> col("newData.empName"),"sal" -> col("newData.sal"),"depId" -> col("newData.depId"),"depName" -> col("newData.depName")))
      .execute()

    deltaTable.toDF.show()

    val df1 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
    df1.show()

    val df2 = spark.read.format("delta").load("/tmp/delta-table")
    df2.show()
  }

  def process():Unit={
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ReadS3")
      .getOrCreate()

    //TODO Need to paas file path in order to read table from bucket

    val s3PathForTables = List("")
    /*val destinationPath
    val dataframes = s3PathForTables.map(readS3Bucket(_, spark))
    dataframes.map(writeDFToS3Bucket(destinationPath, _))*/


  }

  def readS3Bucket(path:String, sparkSession: SparkSession):DataFrame={
    val df = sparkSession.read.parquet(path)
    df
  }

  def writeDFToS3Bucket(path:String, df: DataFrame): Unit={
    df.write.parquet(path)
  }
}
