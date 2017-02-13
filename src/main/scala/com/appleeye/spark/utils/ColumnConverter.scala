package com.appleeye.spark.utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by chglu on 5/26/16.
  */
class ColumnConverter(
    val inputData: DataFrame,
    val inputColumnName: String,
    val outputColumnName: String,
    val splitChar: Char,
    val sqlContext: SQLContext) extends Serializable {

  def listToString (): DataFrame = {
    def strMaker(s: Seq[String]): String = {
      if (s == null) {
        ""
      } else {
        s.mkString(splitChar.toString)
      }
    }

    val strMakeUDF = udf(strMaker(_: Seq[String]))

    inputData.withColumn(outputColumnName, strMakeUDF(inputData(inputColumnName)))
  }


  def stringToList (): DataFrame = {

    def strSpliter(str: String):Array[String] = {
      str.split('|').filter(_ != "")
    }

    val strSplitUDF = udf(strSpliter(_: String))
    inputData.withColumn(outputColumnName, strSplitUDF(inputData(inputColumnName)))
  }

}