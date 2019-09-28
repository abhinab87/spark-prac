package com.abhinab.sparksql

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions.{date_add, _}
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by abhin on 4/16/2019.
  */
object App3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("asd").getOrCreate()
    import spark.implicits._
    val cur_file_path = "C:\\Users\\abhin\\Desktop\\cur_file.csv"
    val pattern = "dd-mm-yyyy"
    val simpleDateFormat = new SimpleDateFormat(pattern)

    val nrch = spark.sparkContext.parallelize(Array[(String, String)](
      ("1001", "2003"), ("1002", "2006"), ("1003", "103"), ("1005", "2005"))).toDF("empId", "mgrId")
    val table = spark.sparkContext.parallelize(Source.fromFile(cur_file_path).getLines().map(_.toString.split(","))
      .map(x => Files(x(0), x(1),x(2),x(3))).toList).toDF
      val rsft_supvr_hrd_xref = table.withColumn("from_date",to_date(table("fromDate"))).withColumn("to_Date",to_date(table("toDate")))
    //rsft_supvr_hrd_xref.show(false)
    // filter out records that are already in Redshift (get inserts)
    val nrch_inserts = nrch.as("nrch").join(rsft_supvr_hrd_xref.as("rsft"),
      $"nrch.empId" <=> $"rsft.empId", "left")
      .where($"rsft.empId".isNull)
      .select($"nrch.*")
    //nrch_inserts.show(false)

    val nrch_inserts1 = nrch.join(rsft_supvr_hrd_xref,nrch("empId") === rsft_supvr_hrd_xref("empId") && nrch("mgrId") === rsft_supvr_hrd_xref("mgrId"),"inner")
      .where($"to_date" === "2199-12-31").select(rsft_supvr_hrd_xref("empId"),rsft_supvr_hrd_xref("from_date"),rsft_supvr_hrd_xref("to_date"),rsft_supvr_hrd_xref("mgrId"))

    // filter out records that are unchanged between nrch and Redshift (get updates)
    val rsft_updates_1 = rsft_supvr_hrd_xref.as("rsft").join(nrch.as("nrch"),
      $"rsft.empId" <=> $"nrch.empId", "left")
      .where($"nrch.empId".isNull)// && $"rsft.to_Date" === "2199-12-31"
      .select($"rsft.*")
    rsft_updates_1.show(false)
    val rsft_update_1 = rsft_updates_1.drop($"to_date").withColumn("to_Date",date_add(lit("2019-04-16"), -1))

    val rsft_updates = rsft_supvr_hrd_xref.as("rsft").join(nrch.as("nrch"),
      $"nrch.empId" <=> $"rsft.empId", "inner")//.where($"to_date" === "2199-12-31")
      .select($"rsft.*").distinct()

    //rsft_updates.show(false)
    // output the count
    //spark.sparkContext.parallelize(Seq(nrch_inserts.count)).coalesce(1,true).saveAsTextFile(s"/tmp/${conf.SUP_COUNT_DIR}")
    //println(s"lr_inserts: ${nrch_inserts.count}")
    //println(s"lr_updates: ${rsft_updates.count}")
    // LR files; apply HRDW values
    val lr_inserts = nrch_inserts.union(nrch).select($"empId",
      lit("2019-04-16").alias("actn_dt"),
      lit("2199-12-31").alias("actn_end_dt"),
      $"mgrId").distinct()
    val lr_inserts2 = lr_inserts.as("ins").join(nrch_inserts1.as("nrch"),$"ins.empId" === $"nrch.empId" && $"ins.mgrId" === $"nrch.mgrId","left")
      .where($"nrch.from_date".isNull).select($"ins.*")
    val lr_inserts3 = lr_inserts2.union(nrch_inserts1)
    //lr_inserts3.show(false)
    //lr_inserts.show(false)
    val lr_updates = rsft_updates.union(rsft_updates_1).withColumn("new_end_dt", when($"to_date" === "2199-12-31", date_add(lit("2019-04-16"), -1)) otherwise($"to_date"))
      .select($"empId", $"mgrId", $"from_date", $"new_end_dt")

    val lr_updates2 = lr_updates.join(nrch_inserts1,lr_updates("empId") === nrch_inserts1("empId") && lr_updates("mgrId") === nrch_inserts1("mgrId"),"left")
      .where(nrch_inserts1("to_date").isNull)
    .select(lr_updates("empId"),lr_updates("mgrId"), lr_updates("from_date"),lr_updates("new_end_dt"))
    //lr_updates2.show(false)
    //nrch_inserts1.show(false)

    //println(s"lr_inserts.count: ${nrch_inserts.count}")
    //println(s"lr_updates.count: ${rsft_updates.count}")
  }
}
case class Files(empId:String,fromDate:String,toDate:String,mgrId:String)