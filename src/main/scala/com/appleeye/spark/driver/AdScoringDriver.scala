package com.appleeye.spark.driver

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by chglu on 16/2/23.
 */
object AdScoringDriver {
  def main(args: Array[String]): Unit = {
    val inputData_ = args(0)
    var startTime_ = args(1)
    var outputPath_ = args(2)
    val siteId_ = args(3) // "idsite list, 845216, 845217, e.g."
    val excludedPixelId_ = args(4) // pixel id stored in custom_var_k1 should be excluded, like ads creative pixel id (5,6)
    val saveFormat_ = "json"
    val timeWindows_ = List((0,1), (1,4), (4,24), (24,720))

    val siteIdList = siteId_.replaceAll(" ","").split(',')

    val conf = new SparkConf().setAppName("Ad Scoring")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val sqlContext_ = new HiveContext(sc)

    print("\n=================================start feature extraction=========================\n")

    print("\n=================================start scoring=========================\n")

    print("\n=================================start saving output=========================\n")

  }
}
