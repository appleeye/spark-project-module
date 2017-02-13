package com.appleeye.spark.entity

/**
  * Created by liuxiao on 3/27/16.
  */
case class Company(industries:Seq[String], imageUrl:String, domain:String, fax:String,
                  name:String, revenue :Int, employees:Int, locations:Seq[Location],
                   linkedIn:LinkedIn, phone:String, updateTime:Double, sources:Seq[Source],
                    technologies:Seq[String])

case class Location(country:String, state:String, street:String, zip:String,
                    city:String)
case class LinkedIn(url:String, id:String, name:String)
case class Source(name:String, id:String)


