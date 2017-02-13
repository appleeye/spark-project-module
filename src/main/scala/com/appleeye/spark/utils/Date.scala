package com.appleeye.spark.utils

import com.github.nscala_time.time.Imports._
import org.joda.time.Days

/**
 * Created by chglu on 16/3/29.
 */
object Date {
  def calculateDatePartition (startTime:String, start: Int, end: Int): String = {
    // generate string like "date=2016-03-11 or date=2016-03-12"
    val baseTime = DateTime.parse(startTime.replace(" ","T"))
    val startDate = DateTime.parse(baseTime.minusHours(end).toLocalDate.toString)
    val endDate = DateTime.parse(baseTime.minusHours(start).toLocalDate.toString)
    val days = Days.daysBetween(startDate, endDate).getDays
    (0 to days map(x => s"date='${endDate.minusDays(x).toLocalDate}'")).mkString(" or ")
  }
}
