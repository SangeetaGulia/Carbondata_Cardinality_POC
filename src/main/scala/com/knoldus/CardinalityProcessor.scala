package com.knoldus

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StringType}

case class CardinalityMatrix(columnName: String, cardinality: Double, columnDataframe: DataFrame, dataType: DataType = StringType)

class CardinalityProcessor{

  val dataFrameUtil = new DataFrameUtil

  def computeCardinality(colName: String, colDataframe: DataFrame): Double = {
    println(s"Computing Cardinality for Column $colDataframe")
    val uniqueData = colDataframe.distinct
    uniqueData.count.toDouble / colDataframe.count
  }

  //TODO: Validation needs to be added so that no two columns in csv have same name
  def setDataTypeWithCardinality(cardinalityMatrixList: List[CardinalityMatrix], inputFileSchemaList: List[CsvHeaderSchema]): List[CardinalityMatrix]= {
    cardinalityMatrixList map { cardinalityMatrix =>
      val filteredColumnHeader: Option[CsvHeaderSchema] = inputFileSchemaList.find { inputFileSchema =>
        inputFileSchema.columnName == cardinalityMatrix.columnName
      }

      filteredColumnHeader match {
        case Some(columnHeader) => cardinalityMatrix.copy(dataType = columnHeader.dataType)
        case _ => throw new Exception("Column Mismatch occured !!")
      }
    }
  }

  /**
    * This method returns the list of cardinality of each column
    * @param dataframe
    * @return
    */
  def getCardinalityMatrix(dataframe: DataFrame): List[CardinalityMatrix] = {
      val cardinalityProcessor = new CardinalityProcessor()
      val cardinalityMatrixList = dataFrameUtil.getColumnNames(dataframe) map { colName =>
      val columnDataframe = dataframe.select(colName)
      val cardinality = cardinalityProcessor.computeCardinality(colName, dataframe.select(colName))
      CardinalityMatrix(colName, cardinality, columnDataframe)
    }
    val inputFileSchema = dataFrameUtil.getColumnDataTypes(dataframe)
    cardinalityProcessor.setDataTypeWithCardinality(cardinalityMatrixList, inputFileSchema)
  }

}
