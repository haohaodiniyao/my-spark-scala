package com.example.test.csv

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object ReadCSV extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logError(
        """
          |请输入读取的csv路径
          |""".stripMargin)
      System.exit(1)
    }
    logWarning("参数 = " + args(0))
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    spark.read.option("header", false).csv(args(0)).show(false)
    spark.stop()
  }
}
