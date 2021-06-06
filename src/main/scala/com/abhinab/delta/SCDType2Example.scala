package com.abhinab.delta

import java.sql.Date

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object SCDType2Example {

  implicit def date(str: String): Date = Date.valueOf(str)

  /**
   * Updated Customers table
   * For customer 1, previous address was update as current = false and new address was inserted as current = true.
   * For customer 2, there was no update.
   * For customer 3, the new address was same as previous address, so no update was made.
   * For customer 4, new address was inserted.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test").master("local[*]").getOrCreate()
    import spark.implicits._

    val CustomerData = Seq(
      Customer(1, "old address for 1", false, null, "2018-02-01"),
      Customer(1, "current address for 1", true, "2018-02-01", null),
      Customer(2, "current address for 2", true, "2018-02-01", null),
      Customer(3, "current address for 3", true, "2018-02-01", null)
    ).toDF().write.format("delta").mode("overwrite").saveAsTable("customers")

    val customerUpdateData = Seq(
      CustomerUpdate(1, "new address for 1", "2018-03-03"),
      CustomerUpdate(3, "current address for 3", "2018-04-04"),    // new address same as current address for customer 3
      CustomerUpdate(4, "new address for 4", "2018-04-04")
    ).toDF()

    val customersTable: DeltaTable =   // table with schema (customerId, address, current, effectiveDate, endDate)
      DeltaTable.forName("customers")

    val updatesDF = customerUpdateData        // DataFrame with schema (customerId, address, effectiveDate)

    // Rows to INSERT new addresses of existing customers
    val newAddressesToInsert = updatesDF
      .as("updates")
      .join(customersTable.toDF.as("customers"), "customerid")
      .where("customers.current = true AND updates.address <> customers.address")

    // Stage the update by unioning two sets of rows
    // 1. Rows that will be inserted in the `whenNotMatched` clause
    // 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
    val stagedUpdates = newAddressesToInsert
      .selectExpr("NULL as mergeKey", "updates.*")   // Rows for 1.
      .union(
        updatesDF.selectExpr("updates.customerId as mergeKey", "*")  // Rows for 2.
      )

    // Apply SCD Type 2 operation using merge
    customersTable
      .as("customers")
      .merge(
        stagedUpdates.as("staged_updates"),
        "customers.customerId = mergeKey")
      .whenMatched("customers.current = true AND customers.address <> staged_updates.address")
      .updateExpr(Map(                                      // Set current to false and endDate to source's effective date.
        "current" -> "false",
        "endDate" -> "staged_updates.effectiveDate"))
      .whenNotMatched()
      .insertExpr(Map(
        "customerid" -> "staged_updates.customerId",
        "address" -> "staged_updates.address",
        "current" -> "true",
        "effectiveDate" -> "staged_updates.effectiveDate",  // Set current to true along with the new address and its effective date.
        "endDate" -> "null"))
      .execute()
  }
}

case class CustomerUpdate(customerId: Int, address: String, effectiveDate: Date)
case class Customer(customerId: Int, address: String, current: Boolean, effectiveDate: Date, endDate: Date)