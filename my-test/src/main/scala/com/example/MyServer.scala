package com.example

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MyServer extends Logging{
  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println(
        """
          |请输入正确的参数
          |1
          |2
          |3""".stripMargin
      )
      System.exit(-1)
    }
    logWarning("参数:"+args(0))
    val sparkBuilder = SparkSession.builder().appName(this.getClass.getName)
    val spark = sparkBuilder.getOrCreate()
    spark.stop()
  }

  def run(spark: SparkSession, arg: String): Unit = {
    logWarning("param = " + arg)
    var a:String = "1,2,3"
    var b:String = "3,4,5"
    for(col <- a.split(",")){
      if(b.contains(col)){
        logWarning("包含"+col)
      }
    }
    spark.read.csv("D:\\csv\\123.csv").limit(10).show()
  }


}
