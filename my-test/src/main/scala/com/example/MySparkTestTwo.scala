package com.example

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MySparkTestTwo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getName)
    val spark = sparkSession.getOrCreate()

    val dfa: DataFrame = spark.read.option("header","true").csv("D:\\file\\csv\\hello.csv")
    val dfb: DataFrame = spark.read.option("header","true").csv("D:\\file\\csv\\world.csv")
//    dfa.show(true)
//    dfb.show(true)
//    dfa.join(dfb,dfa("a")===dfb("a"),"inner").show(true)
    dfa.createOrReplaceTempView("table_a")
    val dfc: DataFrame = spark.sql(String.format(
      """
        |select a,b,c,'%s' as dt from table_a where a=1""".stripMargin,"20220324"))
//    dfc.show(false)
    dfc.repartition(1).write.mode(SaveMode.Overwrite).format("parquet").option("header","false").save("D:\\file\\csv")
    spark.stop()
  }
}
