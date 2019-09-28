package com.abhinab.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by abhin on 9/24/2017.
  */
object JSONFileReading {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JSON-SPARK-SQL").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val path = "C:/Users/abhin/Desktop/1.json"
    val file = sqlContext.read.json(path)
    //file.show()
    /*val df1 = sc.makeRDD(1 to 5).map(x => (x,x*2)).toDF("single","double")
     df1.write.parquet("C:/Users/abhin/Desktop/Test_Table/key=1")
     val df2 = sc.makeRDD(6 to 10).map(x => (x,x*3)).toDF("single","triple")
     df2.write.parquet("C:/Users/abhin/Desktop/Test_Table/key=2")
     val df3=sqlContext.read.parquet("C:/Users/abhin/Desktop/Test_Table/")
     df3.printSchema()
     df3.registerTempTable("student1")
     sqlContext.sql("insert overwirite student select studentName, studentId from student1")
     */
    file.show
    file.printSchema()
    file.filter(file("Age") > 30).show(32)
    file.map(x => "Name: "+x(1)).show()
    /*val personalInfo = sc.textFile("C:/Users/abhin/Desktop/PersonalInfo.txt").map(_.split(", ")).map(x => Row(x(0),x(1),x(2),x(3)))
    val schemaString = "firstName,lastName,city,age"
    val schema = StructType(schemaString.split(",").map(x => StructField(x,StringType,true)))
    val person = sqlContext.createDataFrame(personalInfo,schema)
    person.show()
    person.printSchema()*/
  }
}
