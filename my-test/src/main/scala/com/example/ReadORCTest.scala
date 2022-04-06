package com.example

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object ReadORCTest extends Logging{
  def run(spark: SparkSession): Unit = {
    val path = ".snappy.orc"
    val df = spark.read.orc(path).selectExpr("language", "gender").where("language is not null")
    df.createOrReplaceTempView("df")
    df.show(10,false)
    //language:struct<source:string,confidence:string,value:map<string,float>>
    //gender:struct<value:string,confidence:string,source:string>
    //hive函数map_keys
    val dataFrame = spark.sql(
      """
        |   select map_keys(language.value)[0] as language,
        |       gender.value as gender
        |       from df
        |""".stripMargin)
    dataFrame.show(10,false)
  }
}
