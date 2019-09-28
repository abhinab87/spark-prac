package com.abhinab.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/*import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse*/

/**
  * Created by abhin on 5/13/2019.
  */
object App4 {
  def main(args: Array[String]): Unit = {
    /*implicit val format = DefaultFormats
    val json = "{\"fields\": [{\"fieldName\": \"supId\",\"fieldType\": \"String\",\"length\": 4}, {\"fieldName\": \"hrId\",\"fieldType\": \"String\",\"length\": 4}] }"
    val inputArg = parse(json).extract[InputFormat]*/
    val spark = SparkSession.builder.master("local[*]").appName("asd").getOrCreate()
    import spark.implicits._
    val cur_file_path = "C:\\Users\\abhin\\Desktop\\xxx.csv"
    val df = spark.read.format("csv").option("header","true").load(cur_file_path)
    df.show()
    //println(inputArg)
  }

  def emptyCol = udf((str:String) => if(str.isEmpty || str == null) true else false)
}

case class InputFormat(fields:Array[Field])
case class Field(fieldName:String, fieldType:String, format:Option[String], length:Option[Int])
