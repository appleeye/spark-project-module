package com.appleeye.spark.prototype

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.{ArrayBuffer, Map => MutMap}

/**
 * Created by chglu on 16/2/23.
 */

/**
 * Calculate scores of cookie, and rank
 * @param inputData input dataframe
 * @param timeWindows time windows, like List((0,1), (1,4), (4,24), (24,720))
 * @param frequencyPercentilePoints percentiles points used in percentile calculating of frequency score buckets
 * @param engagementPercentilePoints percentiles points used in percentile calculating of engagement score buckets
 * @param enableIndependentBucket if false, generate buckets of score based on overall websites; otherwise, generate independent bucket for each website
 * @param sqlContext must be a HiveContext, in order to use UDF
 */
class CookieRanking(
                     val inputData: DataFrame,
                     val timeWindows: Seq[(Int, Int)],
                     val frequencyPercentilePoints: Seq[Double] = List(0.25, 0.5, 0.75),
                     val engagementPercentilePoints: Seq[Double] = List(0.25, 0.5, 0.75),
                     val enableIndependentBucket: Boolean = false,
                     val sqlContext: SQLContext) extends Serializable {

  // convert string to column
  import sqlContext.implicits._

  def predictRecencyScore(r: Row): Int = {
    val recencyFeatures = timeWindows.map{
      case (start, end) =>
        r.getAs[Boolean](s"recency_${start}_${end}")
    }

    val recencyScore = {
      if (recencyFeatures(0)) {
        4
      } else if (recencyFeatures(1)) {
        3
      } else if (recencyFeatures(2)) {
        2
      } else if (recencyFeatures(3)) {
        1
      } else {
        throw new Exception("Values of recency features have error: all of them are equals to false/zero")
      }
    }
    recencyScore
  }

  def predictFrequencyScore(r: Row): Int = {
    val frequencyFeatures = timeWindows.map{
      case (start, end) =>
        r.getAs[Long](s"frequency_${start}_${end}")
    }

    val rawFrequencyScore = frequencyFeatures(0) * 4 + frequencyFeatures(1) * 3 + frequencyFeatures(2) *2 + frequencyFeatures(3)
    rawFrequencyScore.toInt
  }

  def bucketizeByPercentile(df: DataFrame, col: String, percentilePoints: Seq[Double], newColumnName:String): DataFrame = {
    df.registerTempTable("temp_percentile")
    val percentilePointsArray = percentilePoints.toList.toString.replace("List","array")

    val buckets = if (enableIndependentBucket) {
        // if the type of column is non integer, should use percentile_approx rather than percentile
        sqlContext.sql(s"select site_id, percentile_approx(cast($col as double), $percentilePointsArray) from temp_percentile group by site_id")
          .map(r => (r.getInt(0), r.getSeq[Double](1))).collect().toMap
    } else {
      val percentiles = sqlContext.sql(s"select percentile_approx(cast($col as double), $percentilePointsArray) from temp_percentile").first().getSeq[Double](0)
      Map(-1 -> percentiles)
    }

    println(newColumnName)
    println(buckets.foreach(println(_)))

    val enableIndependentBucket_ = enableIndependentBucket // avoid serializing this whole object
    val bucketConverter = udf(convertToBuckets(_: Int, _:Int, buckets, enableIndependentBucket_))
    df.withColumn(newColumnName, bucketConverter($"site_id", df(col)))
  }

  def bucketizeByIndex(df:DataFrame, sortKeys:Seq[String], nBuckets:Int, newColumnName:String): DataFrame = {
    val sortedDF = df.sort(sortKeys.head, sortKeys.tail:_*)

    // Seq((site_id, counts of es_id))
    val totals = sortedDF.select("site_id", "es_id")
      .groupBy("site_id").agg(count("es_id")).map(r=> (r.getInt(0), r.getLong(1))).collect().sortBy(_._1)

    val values = sortedDF.rdd.zipWithIndex.map(x => Row.fromSeq(x._1.toSeq :+ (x._2+1).toInt)) // indexs begin from 0, have to plus 1
    val indexedDF = sqlContext.createDataFrame(values, StructType(sortedDF.schema :+ StructField("tmp_index", IntegerType, true)))
    //indexedDF.show()

    var buckets = MutMap[Int, Seq[Double]]()
    for (i <- 0 to totals.size -1) {
      val idxs_ = ArrayBuffer[Double]()
      val total = totals(i)._2
      val prevTotal = if (i > 0) totals(i-1)._2 else 0
      val nBuckets_ = if (nBuckets > total) total.toInt else nBuckets
      for (j <- 1 to nBuckets_ - 1)
        idxs_ += math.ceil(j * total.toDouble / nBuckets_)
      buckets += totals(i)._1 -> idxs_.map(_+prevTotal)
    }

    println(newColumnName)
    println(buckets.foreach(println(_)))

    val bucketConverter = udf(convertToBuckets(_: Int, _:Int, buckets.toMap, true)) // enableIndependentBucket always = true
    indexedDF.withColumn(newColumnName, bucketConverter($"site_id", $"tmp_index")).drop("tmp_index")
  }

  def convertToBuckets(siteId:Int, value: Int, bucketMap: Map[Int,Seq[Double]], enableIndependentBucket: Boolean) = {
    val buckets = if (enableIndependentBucket) bucketMap(siteId) else bucketMap.head._2
    val idx = java.util.Arrays.binarySearch(buckets.toArray, value)
    if (idx >= 0)
      idx + 1
    else
      math.abs(idx)
  }


  def run(): DataFrame = {
    val outputSchema = StructType(inputData.schema ++
      StructType(Seq(
        StructField("recency_score", IntegerType, true),
        StructField("frequency_score", IntegerType, true))))

    val scoreDF = {
      sqlContext.createDataFrame(inputData.map(r=>Row.fromSeq(r.toSeq ++ Seq(predictRecencyScore(r), predictFrequencyScore(r)))), outputSchema)
    }

    val frequencyScoreDF = bucketizeByPercentile(scoreDF, "frequency_score", frequencyPercentilePoints, "frequency_score_four_buckets")

    val engagementScoreDF = frequencyScoreDF.withColumn("engagement_score", $"recency_score" + $"frequency_score")

    val engagementScoreDF2 = bucketizeByPercentile(engagementScoreDF, "engagement_score", engagementPercentilePoints, "engagement_score_four_buckets")
    engagementScoreDF2
    //bucketizeByIndex(engagementScoreDF2, List("site_id", "engagement_score", "recency_score"), 4, "engagement_score_four_buckets_index")
  }
}
