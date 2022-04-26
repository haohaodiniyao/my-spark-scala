package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
  * 读kafka
  *
  */
object ReadKafka extends Logging{
  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      logError(
        """
          |请输入kafka.bootstrap.servers的值 例如 xxxx:9092
          |""".stripMargin)
      System.exit(1)
    }
    logWarning("参数 = "+args(0))
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()

    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", args(0))
      .option("subscribe", "text_topic")
      //-2 earliest
      .option("startingOffsets", """{"text_topic":{"0":1}}""")
      //-1 latest
      .option("endingOffsets", """{"text_topic":{"0":10}}""")
      .load()

    df.printSchema()
    df.selectExpr("cast(key as string)","cast(value as string)","topic").show(300,false)


    spark.stop()
  }
}
