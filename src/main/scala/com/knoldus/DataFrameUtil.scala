package com.knoldus

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

case class CsvHeaderSchema(columnName: String, dataType: DataType, isNullable: Boolean)

class DataFrameUtil {

  def ifHeaderExists(dataFrame: DataFrame) = {
    //TODO: write code to test if header exists
    dataFrame
  }

  def getColumnNames(dataFrame: DataFrame): List[String] = dataFrame.columns.toList

  def getColumnDataTypes(dataFrame: DataFrame): List[CsvHeaderSchema] = {
    dataFrame.schema.toList map {
      colStructure => CsvHeaderSchema(colStructure.name,colStructure.dataType, colStructure.nullable)
    }
  }

  def getColumnHeaderCount(dataFrame: DataFrame) = dataFrame.columns.length

  def processDataFrame(dataframe: DataFrame): Unit = {
    val cardinalityProcessor = new CardinalityProcessor()
    println("processing DataFrame")

    getColumnNames(dataframe) map { colName =>
      dataframe.select(colName).show()
      cardinalityProcessor.computeCardinality(colName, dataframe.select(colName))
    }
  }

}
