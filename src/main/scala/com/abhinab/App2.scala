package com.abhinab

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object App2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("aqw").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(("NetFlix","IND","GOT","V001"), ("NetFlix","IND","FRIENDS","V001"), ("NetFlix","IND","BBT","V001"), ("NetFlix","IND","FRIENDS","V002"), ("NetFlix","IND","FRIENDS","V003"), ("NetFlix","IND","FRIENDS","V005"), ("NetFlix","IND","GOT","V002"), ("NetFlix","IND","GOT","V003"), ("NetFlix","IND","GOT","V004"), ("NetFlix","IND","GOT","V005"), ("NetFlix","USA","GOT","USV001"), ("NetFlix","USA","FRIENDS","USV001"), ("NetFlix","USA","BBT","USV001"), ("NetFlix","USA","FRIENDS","USV002"), ("NetFlix","USA","FRIENDS","USV003"), ("NetFlix","USA","FRIENDS","USV005"), ("NetFlix","USA","FRIENDS","USV006"), ("NetFlix","USA","FRIENDS","USV007"), ("NetFlix","USA","FRIENDS","USV008"), ("NetFlix","USA","GOT","USV005"), ("NetFlix","UK","GOT","UKV001"), ("NetFlix","UK","FRIENDS","UKV001"), ("NetFlix","UK","BBT","UKV001"), ("NetFlix","UK","FRIENDS","UKV002"), ("NetFlix","UK","FRIENDS","UKV003"), ("NetFlix","UK","FRIENDS","UKV005"), ("NetFlix","UK","BBT","UKV002"), ("NetFlix","UK","BBT","UKV003"), ("NetFlix","UK","BBT","UKV004"), ("NetFlix","UK","BBT","UKV005"))
    val TVShowDF = data.toDF("serviceProvider","country","show","viewer")
    val key = TVShowDF.groupBy('serviceProvider).pivot('country).agg(count('viewer))
    key.show(false)

    val data1 = Seq((1,100),(2,200),(3,200),(4,400))
    val datadf = data1.toDF("id", "value")
    val piv = datadf.groupBy('id).pivot('id.alias("newcol")).agg(sum('value))
    piv.show(false)

    val pivotData = Seq(("A","Q1",2000),("B","Q1",1000),("A","Q2",2900),("C","Q2",2800),("D","Q3",2010),("c","Q3",2040),("A","Q4",2300),("C","Q4",2200),("E","Q4",2100),("B","Q4",2100))
    val df3 = pivotData.toDF("name", "quarter", "value")
    val df4 = df3.groupBy('name).pivot('quarter).agg(sum('value))
    df4.show(false)

    val readJSON = spark.read.option("multiline",true).json("/Users/poo/Documents/Support/bhargav/src/main/resources/queries.json")
    readJSON.show(false)
    var state = readJSON.filter(col("type").contains("count") or col("type").contains("median")).filter(!col("query").startsWith("\""))
    state = state.withColumn("suffix",when(col("type")==="count" ,lit("_gbCs_")).when(col("type")==="medianALL","_gbMsALL_").otherwise(lit("_gbMs_")))

    println("***CONCATENATING QUERY AND SUFFIX***")
    state = state.withColumn("query",concat(col("query"),lit(" "),col("suffix"))).repartition(5)

    state.show(false)
    //state = state.withColumn("query", withReplacements(col("query"), abbSeq)).repartition(5)
    //state.show(10)

    val seq = Seq(("D_1_1n","Adult", 100),("D_1_1d","Adult", 100),("D_1_1n","Youth", 100),("D_1_1d","Youth", 100),("D_1_1n","All", 100),("D_1_1d","All", 100)).toDF("id","centerId","count")

    val seqNum = seq.filter('id.endsWith("n")).withColumn("idn", regexp_replace('id,"n",""))
    val seqDenom = seq.filter('id.endsWith("d")).withColumn("idd", regexp_replace('id,"d",""))
    //seqNum.show(false)
    seq.union(seqNum.join(seqDenom, seqDenom("idd") === seqNum("idn") && seqDenom("centerId") === seqNum("centerId") ).select(seqNum("idn"), seqNum("centerId"), seqNum("count")/seqDenom("count").as("count"))).show(false)

    val maps = Map(("a","c") -> Seq(1), ("b","d") ->Seq(2)).transform((k,v) => v.toDF("count")).transform((k,v) => v.select('count).collect().mkString)
      .transform((k,v) => v.replaceAll("\\[","").replaceAll("\\]", "")).toSeq.map(x => (x._1._1,x._1._2,x._2)).toDF
    //maps.show(false)

    val df = Seq(("2019-07-01"),("2019-06-24"),("2019-08-24"),
      ("2018-12-23"),("2018-07-20"))
      .toDF("startDate").select(
      col("startDate"),current_date().as("endDate")
    )

    /*df.withColumn("datesDiff", datediff(col("endDate"),col("startDate")))
      .withColumn("montsDiff", months_between(
        col("endDate"),col("startDate")))
      .withColumn("montsDiff_round",round(months_between(
        col("endDate"),col("startDate")),2))
      .withColumn("yearsDiff",months_between(
        col("endDate"),col("startDate")).divide(12))
      .withColumn("yearsDiff_round",round(months_between(
        col("endDate"),col("startDate")).divide(12),2))
      .show()*/
    df.registerTempTable("df")

    val df1 = spark.sql("select startDate, endDate, datediff(startDate, endDate)/365 from df")
    //df1.show(false)



    val maps2 = Map("AbhinabJena" -> "Abhinab:12", "pooja" -> "13", "Abhinab"->"11")
    val maps1 = maps2.transform((k,v) => if(v.contains(":")) maps2.get(v.split(":")(0)).get+v.split(":")(1) else v)

    println(maps1.getClass)

    val seq1 = Seq(("A",1,0,1),("B",1,1,1),("C",0,0,1),("D",1,0,0)).toDF("id","count1","count2","count3")
    seq1.registerTempTable("seq1")
    spark.sql("select * from seq1 where (count1+ count2 + count3) >= 2").show(false)

    //val seq = Seq((2,110),(2,130),(2,120),(3,200),(3,206),(3,206),(4,150),(4,160),(4,170))
    val rdd = spark.sparkContext.parallelize(Seq((2,110),(2,130),(2,120),(3,200),(3,206),(3,206),(4,150),(4,160),(4,170)))

    rdd.groupBy(_._1).map(x=> (x._1,x._2.map(_._2))).map(x => (x._1, x._2.sum/x._2.size))
//https://github.com/abhinab87/spark-prac.git
  }

  def withReplacements(column: Column, abbSeq: mutable.Map[String, String]): Column =
    abbSeq.foldLeft[Column](column) {
      case (col, (from, to)) => regexp_replace(col, from, to)
    }
}
