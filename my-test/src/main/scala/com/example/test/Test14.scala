package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * socket数据
  */
object Test14 extends Logging{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    //日志级别
    spark.sparkContext.setLogLevel("ERROR")
    //nc -lk 9999
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999").load()

    df.printSchema()
    val wordDF: DataFrame = df.select(explode(split(df("value"), " ")).alias("word"))
    val count: DataFrame = wordDF.groupBy("word").count()

    val query = count.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

    spark.stop()
  }
}
