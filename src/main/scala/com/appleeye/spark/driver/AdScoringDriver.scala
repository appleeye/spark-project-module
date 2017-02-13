package com.appleeye.spark.driver

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by chglu on 16/2/23.
 */
object AdScoringDriver {
  def main(args: Array[String]): Unit = {

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
