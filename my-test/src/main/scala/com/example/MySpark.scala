package com.example

import com.example.util.S3Util
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object MySpark extends Logging{
  val defaultValue: () => String = () => ""
  val addDefaultValueUdf: UserDefinedFunction = udf(defaultValue)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName)
    val spark = sparkSession.getOrCreate()
    val fields = "app,country,app_ver"
    val schema = getSchema(fields)

    val paths = "path1,path2"
    val pathArray:Array[String] = paths.split(",").map(url => S3Util.FormatS3URL(url))
    //option("header", "false")是否将csv文件第一行作为schema
    val dataFrame:DataFrame = spark.read.option("header", "false").schema(schema).csv(pathArray: _*)
    val parquetPath = ""
    var dataFrame1 = spark.read.option("header", "false").schema(schema).parquet(parquetPath)
    dataFrame1 = dataFrame1.withColumn("col",addDefaultValueUdf())
    val dataFrame2 = spark.read.option("header", "false").schema(schema).parquet(pathArray: _*)
    val dataFrame3 = dataFrame1.union(dataFrame2)
    spark.stop()
  }

  def getSchema(fields: String): StructType = {
    val schema: StructType = StructType(fields.split(",").map(field => {
      if (field == "app_ver" || field == "os_ver") {
        StructField(field, LongType, nullable = true)
      } else {
        StructField(field, StringType, nullable = true)
      }
    }))
    schema
  }
}
