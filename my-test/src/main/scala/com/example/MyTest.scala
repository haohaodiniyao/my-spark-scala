package com.example

import com.alibaba.fastjson.JSON
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object MyTest extends Logging{
  def main(args: Array[String]): Unit = {
    logInfo(
      """
        |hello
        |world""".stripMargin)

    val sparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getName)
    val spark = sparkSession.getOrCreate()

    val param = "{'app':'weixin','country':'CN'}"
    val paramJSON = JSON.parseObject(param)
    logInfo("app = "+paramJSON.getString("app"))
    val cols = "app,country"
    for(col <- cols.split(",")){
      logInfo("col = "+col)
    }
    logInfo("cols contains p,c "+cols.contains("p,c"))
    spark.stop()
  }
}
