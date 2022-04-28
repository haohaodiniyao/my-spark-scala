package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * ParquetAvroWriters.forReflectRecord(MsgData.class)
  */
object ReadParquet extends Logging{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    val df1: DataFrame = spark.read.parquet("/Users/shareit/Downloads/tmp/output/dt=20220424")
    logWarning("count1 = "+df1.count())
    spark.stop()
  }
}
