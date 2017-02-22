package com.knoldus

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sangeeta on 22/2/17.
  */
object DataFrameHandler {

  def main(args: Array[String]) {
    System.out.println("Staring with the demo project")
    val conf = new SparkConf().setAppName("cardinality_demo").setMaster("local")
    val sc = new SparkContext(conf)

      val sqlContext = new SQLContext(sc)
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true") // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .load("src/main/resources/user.csv")
      df.show()
      df.printSchema()
    }
}
