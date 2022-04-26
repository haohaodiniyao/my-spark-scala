
package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * 写kafka
  */
object WriteDataFrameToKafka extends Logging{

  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      logError(
        """
          |请输入kafka.bootstrap.servers的值 例如 xxxx:9092
          |""".stripMargin)
      System.exit(1)
    }
    logWarning("参数 = "+args(0))
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq (("iphone", "2007"),("iphone 3G","2008"),
      ("iphone 3GS","2009"),
      ("iphone 4","2010"),
      ("iphone 4S","2011"),
      ("iphone 5","2012"),
      ("iphone 8","2014"),
      ("iphone 10","2017"))

    val df = spark.createDataFrame(data).toDF("key","value")
    /*
      since we are using dataframe which is already in text,
      selectExpr is optional. 
      If the bytes of the Kafka records represent UTF8 strings, 
      we can simply use a cast to convert the binary data 
      into the correct type.

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    */
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers",args(0))
      .option("topic","text_topic")
      .save()
  }
}
