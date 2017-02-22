package com.knoldus

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameHandler {

  def loadData(): DataFrame ={
    println("Starting with the demo project")
    val conf = new SparkConf().setAppName("cardinality_demo").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val df: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/user.csv")
    df.show()
    df.printSchema()
    df
  }


  def main(args: Array[String]) {

    val dataFrame = loadData()
    val dataFrameUtil = new DataFrameUtil()

    println("Column count of CSV is >>>>>> " + dataFrameUtil.getColumnHeaderCount(dataFrame))
    println("Column names of CSV is >>>>>> " + dataFrameUtil.getColumnNames(dataFrame))

    dataFrameUtil.processDataFrame(dataFrame)

  }
}
