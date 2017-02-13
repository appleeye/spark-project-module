package com.appleeye.spark.prototype

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

/**
 * Created by chglu on 16/2/25.
 */

class CookieRankingTest extends FunSuite with SharedSparkContext {


/**
output
scala> computePercentile(sc.parallelize(List((0),(2),(3))).toDF("x"),"x",List(0.25, 0.5, 0.75))
res69: Map[Int,Seq[Double]] = Map(-1 -> WrappedArray(0.0, 1.0, 2.25))

scala> computePercentile(sc.parallelize(List((1),(7),(9),(10))).toDF("x"),"x",List(0.25, 0.5, 0.75))
res59: Map[Int,Seq[Double]] = Map(-1 -> WrappedArray(1.0, 7.0, 9.0))

scala> computePercentile(sc.parallelize(List((0),(2),(3),(1),(7),(9),(10))).toDF("x"),"x",List(0.25, 0.5, 0.75))
res70: Map[Int,Seq[Double]] = Map(-1 -> WrappedArray(0.75, 2.5, 7.5))

enableIndependentBucket = false:
+-------+-------------+-------------+--------------+----------------+-----------+-----------+------------+--------------+-------------+---------------+----------------------------+----------------+-----------------------------+
|site_id|frequency_0_1|frequency_1_4|frequency_4_24|frequency_24_720|recency_0_1|recency_1_4|recency_4_24|recency_24_720|recency_score|frequency_score|frequency_score_four_buckets|engagement_score|engagement_score_four_buckets|
+-------+-------------+-------------+--------------+----------------+-----------+-----------+------------+--------------+-------------+---------------+----------------------------+----------------+-----------------------------+
|      1|            0|            0|             0|               0|       true|      false|       false|         false|            4|              0|                           1|               4|                            2|
|      1|            0|            1|             0|               0|      false|       true|       false|         false|            3|              3|                           3|               6|                            3|
|      1|            0|            0|             1|               0|      false|      false|        true|         false|            2|              2|                           2|               4|                            2|
|      2|            0|            0|             0|               1|      false|      false|       false|          true|            1|              1|                           2|               2|                            1|
|      2|            1|            1|             0|               0|       true|       true|       false|         false|            4|              7|                           3|              11|                            3|
|      2|            1|            1|             1|               0|       true|       true|        true|         false|            4|              9|                           4|              13|                            4|
|      2|            1|            1|             1|               1|       true|       true|        true|          true|            4|             10|                           4|              14|                            4|
+-------+-------------+-------------+--------------+----------------+-----------+-----------+------------+--------------+-------------+---------------+----------------------------+----------------+-----------------------------+


enableIndependentBucket = true
engagement_score_4 refer to method bucketizeByIndex
+-------+-----+-------------+-------------+--------------+----------------+-----------+-----------+------------+--------------+-------------+---------------+----------------------------+----------------+-----------------------------+-----------------------------------+
|site_id|es_id|frequency_0_1|frequency_1_4|frequency_4_24|frequency_24_720|recency_0_1|recency_1_4|recency_4_24|recency_24_720|recency_score|frequency_score|frequency_score_four_buckets|engagement_score|engagement_score_four_buckets|engagement_score_four_buckets_index|
+-------+-----+-------------+-------------+--------------+----------------+-----------+-----------+------------+--------------+-------------+---------------+----------------------------+----------------+-----------------------------+-----------------------------------+
|      1|    1|            0|            0|             0|               0|       true|      false|       false|         false|            4|              0|                           1|               4|                            2|                                  2|
|      1|    2|            0|            1|             0|               0|      false|       true|       false|         false|            3|              3|                           4|               6|                            4|                                  3|
|      1|    3|            0|            0|             1|               0|      false|      false|        true|         false|            2|              2|                           3|               4|                            2|                                  1|
|      2|    4|            0|            0|             0|               1|      false|      false|       false|          true|            1|              1|                           1|               2|                            1|                                  1|
|      2|    5|            1|            1|             0|               0|       true|       true|       false|         false|            4|              7|                           2|              11|                            2|                                  2|
|      2|    6|            1|            1|             1|               0|       true|       true|        true|         false|            4|              9|                           3|              13|                            3|                                  3|
|      2|    7|            1|            1|             1|               1|       true|       true|        true|          true|            4|             10|                           4|              14|                            4|                                  4|
+-------+-----+-------------+-------------+--------------+----------------+-----------+-----------+------------+--------------+-------------+---------------+----------------------------+----------------+-----------------------------+-----------------------------------+
 * * */
  test("Cookie ranking test") {
    val rootLogger = Logger.getRootLogger()
    //rootLogger.setLevel(Level.ERROR)
    val timeWindows = List((0,1), (1,4), (4,24), (24,720)).zipWithIndex

    val baseSchema = Seq(
      StructField("site_id", IntegerType, false),
      StructField("es_id", IntegerType, false)
    )

    val frequencyFeatureSchema = timeWindows.map {
      case ((start, end), _) =>
        StructField(s"frequency_${start}_${end}", LongType, true)
    }
    val recencyFeatureSchema = timeWindows.map {
      case ((start, end), _) =>
        StructField(s"recency_${start}_${end}", BooleanType, true)
    }

    val inputSchema = StructType(baseSchema ++ frequencyFeatureSchema ++ recencyFeatureSchema)

    val rowRDD = sc.parallelize(Seq(
      Row(1, 1, 0l, 0l, 0l, 0l, true, false, false, false),
      Row(1, 2, 0l, 1l, 0l, 0l, false, true, false, false),
      Row(1, 3, 0l, 0l, 1l, 0l, false, false, true, false),
      Row(2, 4, 0l, 0l, 0l, 1l, false, false, false, true),
      Row(2, 5, 1l, 1l, 0l, 0l, true, true, false, false),
      Row(2, 6, 1l, 1l, 1l, 0l, true, true, true, false),
      Row(2, 7,1l, 1l, 1l, 1l, true, true, true, true)
    ))
    val sqlContext_ = new HiveContext(sc)

    val featureDF = sqlContext_.createDataFrame(rowRDD, inputSchema)
    val scoreDF = new CookieRanking(
      inputData = featureDF,
      timeWindows = List((0,1), (1,4), (4,24), (24,720)),
      frequencyPercentilePoints = List(0.25, 0.5, 0.75),
      enableIndependentBucket = true,
      sqlContext = sqlContext_).run().sort("es_id")
    scoreDF.show()
    assert(scoreDF.map(_.getAs[Int]("recency_score")).collect().toList == List(4,3,2,1,4,4,4))
    assert(scoreDF.map(_.getAs[Int]("frequency_score")).collect().toList == List(0,3,2,1,7,9,10))
  }
}
