package com.example.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test01 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    import spark.implicits._
    val columns: Seq[String] = Seq("language", "users_count")
    val data: Seq[(String, String)] = Seq(("Java", "1000"), ("Python", "2000"), ("Scala", "3000"))
    val rdd: RDD[(String, String)] = spark.sparkContext.parallelize(data)
    val dfFromRDD1: DataFrame = rdd.toDF("language","users_count")
    dfFromRDD1.printSchema()
    spark.stop()
  }
}
