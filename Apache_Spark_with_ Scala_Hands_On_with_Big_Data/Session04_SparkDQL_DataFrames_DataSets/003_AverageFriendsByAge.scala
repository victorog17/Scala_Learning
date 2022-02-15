package com.sundogsoftware.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

object AverageFriendsByAge {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")

    println("Here is our inferred schema:")
    people.printSchema()

    // Selected only the columns needed
    val selectedData = people.select("age", "friends")

    println("Group by age:")
    selectedData.groupBy("age").avg("friends").orderBy("age").show()

    println("Group by age and with avg friends formated:")
    selectedData.groupBy("age").agg(round(avg("friends"), 2))
      .sort("age").show()

    println("Group by age, with avg friends formated and new column name customized:")
    selectedData.groupBy("age").agg(round(avg("friends"), 2)
      .alias("friends_avg")).sort("age").show()
  }

}
