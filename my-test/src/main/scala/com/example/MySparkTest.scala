package com.example

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object MySparkTest extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getName)
    val spark = sparkSession.getOrCreate()

    var df: DataFrame = spark.read.csv("")
    val defaultValue: () => Long = () => 0
    val addDefaultValueUdf: UserDefinedFunction = udf(defaultValue)
    df = df.withColumn("newCol",addDefaultValueUdf()).filter(row=>filter(row))
    logInfo("schema = "+df.schema)
    df.show(10,false)
    val path = ""
//    df.repartition(10).write.mode(SaveMode.Overwrite).option("header","false").save(path)

    val filters:JSONArray = new JSONArray()
    var flag = true
    for(i<-0 until filters.size() if flag){
      var jsonObject: JSONObject = filters.getJSONObject(i)
    }

    spark.stop()
  }

  def filter(row:Row): Boolean ={
    val str = row.getAs[String]("")
    true
  }
}
