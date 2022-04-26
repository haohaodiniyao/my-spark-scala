package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * 2.4.0
  * 支持
  * schema_of_json
  */
object Test11 extends Logging {
  def main(args:Array[String]) : Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
    val data = Seq((1, jsonString))
    import spark.implicits._
    val df=data.toDF("id","value")
    df.show(false)

    import org.apache.spark.sql.functions.{col, from_json}
    import org.apache.spark.sql.types.{MapType, StringType}
    val df2=df.withColumn("value",from_json(col("value"),MapType(StringType,StringType)))
    df2.printSchema()
    df2.show(false)

    import org.apache.spark.sql.functions.to_json
    df2.withColumn("value",to_json(col("value")))
      .show(false)

    import org.apache.spark.sql.functions.json_tuple
    df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City"))
      .toDF("id","Zipcode","ZipCodeType","City")
      .show(false)

    import org.apache.spark.sql.functions.get_json_object
    df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").as("ZipCodeType"))
      .show(false)

//    import org.apache.spark.sql.functions.{lit, schema_of_json}
//    val schemaStr=spark.range(1)
//      .select(schema_of_json(lit("""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}""")))
//      .collect()(0)(0)
//    println(schemaStr)

    spark.stop()
  }
}
