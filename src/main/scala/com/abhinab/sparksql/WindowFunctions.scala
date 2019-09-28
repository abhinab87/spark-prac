package com.abhinab.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object WindowFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("StructuredNetworkWordCount").master("local[*]").getOrCreate()
    import spark.implicits._

    val empsalary = Seq(
      Salary("sales", 1, 5000),
      Salary("personnel", 2, 3900),
      Salary("sales", 3, 4800),
      Salary("sales", 4, 4800),
      Salary("personnel", 5, 3500),
      Salary("develop", 7, 4200),
      Salary("develop", 8, 6000),
      Salary("develop", 9, 4500),
      Salary("develop", 10, 5200),
      Salary("develop", 11, 5200)).toDS

    val byDepName = Window.partitionBy('depName)
    empsalary.withColumn("avg", avg('salary) over byDepName).show


    val dataset = Seq(
      ("Thin",       "cell phone", 6000),
      ("Normal",     "tablet",     1500),
      ("Mini",       "tablet",     5500),
      ("Ultra thin", "cell phone", 5000),
      ("Very thin",  "cell phone", 6000),
      ("Big",        "tablet",     2500),
      ("Bendable",   "cell phone", 3000),
      ("Foldable",   "cell phone", 3000),
      ("Pro",        "tablet",     4500),
      ("Pro2",       "tablet",     6500))
      .toDF("product", "category", "revenue")

    val overCategory = Window.partitionBy('category).orderBy('revenue.desc)
    val data = dataset
    val ranked = data.withColumn("rank", dense_rank.over(overCategory))
    val reveDesc = Window.partitionBy('category).orderBy('revenue.desc)
    val reveDiff = max('revenue).over(reveDesc) - 'revenue
    data.select('*, reveDiff as 'revenue_diff).show(false)
    //ranked.show(false)
    //ranked.where('rank <= 2).show(false)
  }
}
case class Salary(depName: String, empNo: Long, salary: Long)