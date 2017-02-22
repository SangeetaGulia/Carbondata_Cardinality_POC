package com.knoldus

import org.apache.spark.sql.DataFrame

class CardinalityProcessor{

  def computeCardinality(colName: String, colDataframe: DataFrame): Unit = {
    println(s"Computing Cardinality for Column ${colDataframe}")
    val uniqueData = colDataframe.distinct
    val columnCardinality=uniqueData.count.toDouble/colDataframe.count

    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    println("UniqueData is >>>>>>>>>>>  " + uniqueData)
    println("TotalData is >>>>>>>>>>>  " + colDataframe)
    println(s"Cardinality of the column is ${columnCardinality}")
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

  }
}
