package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * ParquetAvroWriters.forReflectRecord(MsgData.class)
  */
object ReadParquet extends Logging{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    val df1: DataFrame = spark.read.parquet("part-0-2")
    val df2: DataFrame = spark.read.parquet("part-0-3")
    val df4: DataFrame = spark.read.parquet("part-0-4")
    val df5: DataFrame = spark.read.parquet("part-0-5")
    logWarning("count1 = "+df1.count()+",count2 = "+df2.count())
    logWarning("count4 = "+df4.count()+",count5 = "+df5.count())
    spark.stop()
  }
}
