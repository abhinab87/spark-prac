package com.abhinab.delta

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types._


object SparkDeltaInsertExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()

    import spark.implicits._
    import io.delta.implicits._

    val path = "/data/events/"
    val data = Seq(("E101", "David", 10000, "david@gmail.com", "Russia"), ("E102", "John", 10000, "John@gmail.com", "UK"),("E103", "Rana", 10000, "Rana@gmail.com", "India")
    ,("E104", "Kushal", 10000, "Kushal@gmail.com", "Canada"),("E105", "Shyam", 10000, "Shyam@gmail.com", "USA"),("E106", "AIM", 10000, "AIM@gmail.com", "Brazil"))

    val df = data.toDF("id", "name", "salary", "email", "country")


    //Saving datafrmae to a delta location
      df
      .repartition(col("country"))
      .write
      .format("delta")
      .partitionBy("country")
      .save(path)

    //Creating new column and append the data to the location
      df
      .transform(withContinent())
      .write
      .format("delta")
      .option("mergeSchema", "true")
      .mode(SaveMode.Append)
      .save(path)

  }

  def withContinent()(df: DataFrame): DataFrame = {
    df.withColumn(
      "continent",
      when(col("country") === "Russia" || col("country") === "UK", "Europe")
        .when(col("country") === "India", "Asia")
        .when(col("country") === "Brazil", "South America")
        .when(col("country") === "Canada" || col("country") === "USA", "North America")
    )
  }
}
case class IncomingObj(key: String, value: Int)