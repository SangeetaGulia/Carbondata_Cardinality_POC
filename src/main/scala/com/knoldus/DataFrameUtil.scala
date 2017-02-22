package com.knoldus

import org.apache.spark.sql.DataFrame


class DataFrameUtil {

  def ifHeaderExists(dataFrame: DataFrame) = {
    //TODO: write code to test if header exists
    dataFrame
  }

  def getColumnNames(dataFrame: DataFrame): List[String] = dataFrame.columns.toList

  def getColumnHeaderCount(dataFrame: DataFrame) = dataFrame.columns.length

  def processDataFrame(dataframe: DataFrame): Unit = {
    val cardinalityProcessor = new CardinalityProcessor()
    println("processing DataFrame")

    getColumnNames(dataframe) map { colName =>
      println(s" Displaying Records from Column : ${colName}")
      dataframe.select(colName).show()
      cardinalityProcessor.computeCardinality(colName, dataframe.select(colName))
    }
  }

}
