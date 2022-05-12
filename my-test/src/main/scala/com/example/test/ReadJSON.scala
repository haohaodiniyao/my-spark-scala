package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * ParquetAvroWriters.forReflectRecord(MsgData.class)
  */
object ReadJSON extends Logging{
  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      logError(
        """
          |请输入文件路径
          |""".stripMargin)
      System.exit(1)
    }
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    val df1: DataFrame = spark.read.json(args(0))
    logWarning("count1 = "+df1.count())
    df1.show(100,false)
    spark.stop()
  }
}
