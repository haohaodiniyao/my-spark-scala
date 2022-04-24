package com.example.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test03 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    val data: Seq[(String, String)] = Seq(("Java", "1000"), ("Python", "2000"), ("Scala", "3000"))
    val rdd: RDD[(String, String)] = spark.sparkContext.parallelize(data)
    val rowRDD: RDD[Row] = rdd.map(attributes => Row(attributes._1, attributes._2))
    val schema: StructType = StructType(Array(StructField("language", StringType, true), StructField("users_count", StringType, true)))
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)
    df.printSchema()
    df.select("*").show()
    spark.stop()
  }
}
