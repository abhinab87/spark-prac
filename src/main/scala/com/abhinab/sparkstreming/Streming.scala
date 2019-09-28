package com.abhinab.sparkstreming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
  * Created by abhin on 9/20/2017.
  */
object Streming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    val lines = ssc.socketTextStream("localhost",18092)
    val words = lines.flatMap(_.split(" "))
    /*words.foreachRDD{rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._
      val wordDF = rdd.toDF("wrd")
      wordDF.registerTempTable("Word")
      sqlContext.sql("select wrd, count(*) as total from Word group by wrd").show()
    }*/
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    lines.print()
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }

}
