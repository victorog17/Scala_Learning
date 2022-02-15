package com.sundogsoftware.spark

import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Find the minimum temperature by weather station
case object TotalAmountSpentVictorDataset {

  case class AmountSpent(ID: Int, productID: Int, price: Float)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("MoneySpent")
      .master("local[*]")
      .getOrCreate()

    val moneySpentSchema = new StructType()
      .add("ID", IntegerType, nullable = true)
      .add("productID", IntegerType, nullable = true)
      .add("price", FloatType, nullable = true)

    // Read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .schema(moneySpentSchema)
      .csv("data/customer-orders.csv")
      .as[AmountSpent]

    // Select only ID and price
    val selectedData = ds.select("ID", "price")
    selectedData.show()

    val personGrouped = selectedData.groupBy("ID")
      .agg(round(sum("price"), 2).alias("sum_price"))
      .sort($"sum_price".desc)
    personGrouped.show()

    // Collect, format, and print the results
    val results = personGrouped.collect()

    for (result <- results) {
      val people = result(0)
      val spentMoney = result(1)
      println(f"$people spent $spentMoney dollars")
    }
  }
}
