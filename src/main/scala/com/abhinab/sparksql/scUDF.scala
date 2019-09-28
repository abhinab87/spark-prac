package com.abhinab.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by abhin on 8/29/2018.
  */
object scUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("XX").master("local[*]").getOrCreate()
    import spark.implicits._
    val df =  Seq(Person("DF101","Abhi", 55.0,45,"Carom,Cricket,TT"),Person("DF101",null, 55.0,45, "Cricket,TT"),Person("DF103","Abhinab", 55.0,45, "TT,Carom"),Person("DF104","Abhishek", 55.0,45, "FootBall")).toDF
    df.show(false)
    val df1 = df.withColumn("ud", xxudf('name, 'sal))
    df1.show(false)
    val df2 = df.withColumn("sports", explode(split('sprtIntrst,","))).groupBy('id,trim('sports)).agg(count('sports)).orderBy('id)
    df2.show(false)
    val df3 = df.withColumn("sports", explode(split('sprtIntrst,","))).groupBy('sports,'id).agg(array('id).as("grpId")).orderBy('sports)
    //val df4 = df3.withColumn("distId", )
    df3.show(false)
  }
  def xxudf = udf((name:String,sal:Int) => (name,sal) match {
    case (name, sal) if(name == null && sal >= 50) => {
      if(name == null && sal >= 45)  "DDD, DDDE" else "DDD"
    }
    case (name, sal) if(name == null && sal >= 45) => "DDDE"
    case (name, sal) if(name.length == 7 && sal >= 45) => "DDE"
    case _ => "SSS"
  })
}

case class Person(id:String, name:String, sal:Double, age:Int, sprtIntrst:String)