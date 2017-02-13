package com.appleeye.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext


/**
 * Created by chglu on 16/2/23.
 */
object Test {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
  }
  val sc = new SparkContext()
  val sqlContext = new HiveContext(sc)
  import sqlContext.implicits._

  val schemaDefined = List("indicator", "name", "category", "subcategory", "display_flag")
  val rawDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true")
    .load("/user/changlu/northstar/727_indicator_mapping.csv")
    .withColumnRenamed("feature", "indicator")
    .withColumnRenamed("n_display_value", "name")
    .withColumnRenamed("UI_displaycat", "category")
    .withColumnRenamed("n_subcat", "subcategory")
    .withColumnRenamed("display flag", "display_flag")


  /*
  *
+--------------------+------+
|       UI_displaycat| count|
+--------------------+------+
|     Company Profile|   360|
|     Web Information|   295|
|              Social|    28|
|       Business Tags|159286|
|             Funding|    17|
|Corporate Relatio...|  1297|
|     Technology Used|  6675|
+--------------------+------+
  *
  * */

  /*

tech_sub_category //delete
tech_name
state // has value
revenue // has value
industry
employees // has value
country
3_gram
2_gram

has_webpage_partner_comp
has_webpage_partner_state
has_webpage_client_comp
has_webpage_client_state


   */

  val filteredDF = rawDF.filter("display_flag = 'y' or display_flag = 'Y'")
  /*
  filteredDF.map(r => {
    r.getAs[String]("feature")
  }).collect.toSet.foreach(println)
  */

  def getCmpNames(df: DataFrame): Map[String,String] = {
    val cmpDF = sqlContext.read.format("orc").load("/data/es/structured/company/dt=2016-07-08")
    val domainDF = df.filter($"feature".contains("has_webpage_partner_comp") or  $"feature".contains("has_webpage_client_comp")).map(r => {
      r.getAs[String]("feature").split('_').last
    }).toDF("domain_1").distinct()
    domainDF.join(cmpDF, domainDF("domain_1") === cmpDF("domain"),"left_outer").map(r=> (r.getAs[String]("domain_1"),r.getAs[String]("name"))).collect().toMap
  }

  val cmpNames = sc.broadcast(getCmpNames(filteredDF))

  def getTechNames(): Map[String, (String, String)] = {
    //val techDF = sqlContext.read.format("orc").load("/user/hua/indicator/20160708/indicator_count.orc")
    //techDF.filter($"feature".contains("tech_name=")).distinct().map(r=> (r.getAs[String]("feature").toLowerCase, r.getAs[String]("feature"))).collect().toMap
    val schemaDefined = List("name", "category", "display_category")
    val techDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("/user/changlu/northstar/tech_blacklist_0811.csv")
      .filter("blacklist != '1'")
      .withColumnRenamed("tech_name", "name")
      .withColumnRenamed("tech_category", "category")
    techDF.registerTempTable("tech_table")
    val techDFGrouped = sqlContext.sql(s"select name, collect_set(category) as category, collect_set(display_category) as display_category from tech_table group by name")
    techDFGrouped.map(r => {
      val cat_list = r.getAs[Seq[String]]("display_category")
      val subcat = if (cat_list.size > 1) r.getAs[Seq[String]]("category").head else cat_list.head
      val name = "tech_name=" + r.getAs[String]("name")
      (name, (name, if (subcat == "undefined") "" else subcat))
    }).collect().toMap
  }

  val techNames = sc.broadcast(getTechNames())

  def getValue(feature: String, subcat: String): String = {
    val (k, v) = {
      if (feature == null) {
        (null, null)
      } else if (feature.contains("=")) {
        val l = feature.split('=')
        (l.head, l.drop(1).mkString("="))
      } else {
        val l = feature.split('_')
        (l.dropRight(1).mkString("_"), l.last)
      }
    }
    k match {
      case "tech_sub_category" => null
      case "tech_name" => if (subcat != "" && subcat != "null") s"Uses $v | $subcat" else s"Uses $v"
      case "industry" => s"Is within the $v industry"
      case "country" => s"Country of business is $v"
      case "2_gram" | "3_gram" => v
      case "has_webpage_partner_state" => s"Has partner in the state of $v"
      case "has_webpage_client_state" => s"Has client in the state of $v"
      case "has_webpage_partner_comp" => s"Has ${cmpNames.value.getOrElse(v,"non-company")} as partner"
      case "has_webpage_client_comp" => s"Has ${cmpNames.value.getOrElse(v,"non-company")} as client"
      case _ => null
    }
  }

  // remove features containing non-ascii chars in display name
  val insightDF = filteredDF.map {
    case r =>
      //val category = r.getAs[String]("category")
      val category = "Technology Used"
      val (feature, subcat) = {
        val f = r.getAs[String]("indicator").replaceAll("\"","")
        val s = r.getAs[String]("subcategory")
        if (f.contains("tech_name=")) {
          techNames.value.getOrElse(f,(null,null))
        } else {
          (f,s)
        }
      }
      val displayValue = r.getAs[String]("name")
      val newDisplayValue = if (displayValue == "") getValue(feature, subcat) else displayValue
      (feature, newDisplayValue, category, subcat)
  }.filter(x => x._1 != null && x._2 != null).filter(x => !x._2.map(c => c>31 && c<127).contains(false)).toDF("indicator", "name", "category", "subcategory")

  insightDF.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(s"/user/changlu/northstar/feature_whitelist_20160728.csv")
}
