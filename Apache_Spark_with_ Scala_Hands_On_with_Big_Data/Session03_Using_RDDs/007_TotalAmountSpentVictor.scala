package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

// Compute the total amount spent per customer in some fake e-commerce data.
object TotalAmountSpentVictor {

  // Convert input data to (customerID, amount Spent) tuples.
  def parseLine(line:String): (Int, Float) = {
    val fields = line.split(",")
    val customer = fields(0).toInt
    val moneySpent = fields(2).toFloat
    (customer, moneySpent)
  }
  // Our main function where the action happens.
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpent")

    val lines = sc.textFile("data/customer-orders.csv")

    val rdd = lines.map(parseLine)
    //rdd.foreach(println)

    //val dataTypes = rdd.map(x => (x._1.toInt, x._2.toFloat))

    val spentMoney = rdd.reduceByKey((x, y) => x + y)
    //val spentMoney = dataTypes.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    //spentMoney.foreach(println)

    //val amountSpent = spentMoney.mapValues(x => x._1)
    //amountSpent.foreach(println)

    val results = spentMoney.collect()

    //results.sorted.foreach(println)
    val resultsSorted = results.sortBy(x => x._2)(Ordering[Float].reverse)

    for (result <- resultsSorted) {
      val customer1 = result._1
      val money = result._2
      val formatMoney = f"$money%.2f"
      println(f"The custome nº $customer1 spent $formatMoney dolars")
    }
  }
}
