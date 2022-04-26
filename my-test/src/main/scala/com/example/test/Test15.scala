package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 读json文件
  * 写到控制台
  */
object Test15 extends Logging{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    if(args.length<1){
      logError(
        """
          |请输入json文件路径
          |""".stripMargin)
      System.exit(1)
    }
    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(
      List(
        StructField("RecordNumber", IntegerType, true),
        StructField("Zipcode", StringType, true),
        StructField("ZipCodeType", StringType, true),
        StructField("City", StringType, true),
        StructField("State", StringType, true),
        StructField("LocationType", StringType, true),
        StructField("Lat", StringType, true),
        StructField("Long", StringType, true),
        StructField("Xaxis", StringType, true),
        StructField("Yaxis", StringType, true),
        StructField("Zaxis", StringType, true),
        StructField("WorldRegion", StringType, true),
        StructField("Country", StringType, true),
        StructField("LocationText", StringType, true),
        StructField("Location", StringType, true),
        StructField("Decommisioned", StringType, true)
      )
    )

    //json路径是目录
    val df = spark.read
      .schema(schema)
      .json(args(0))

//    val df = spark.readStream
//      .schema(schema)
//      .json(args(0))

    df.printSchema()

    val groupDF = df.select("Zipcode")
      .groupBy("Zipcode").count()
    groupDF.printSchema()

    groupDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

//    groupDF.write
//      .format("console").save()

    spark.stop()
  }
}
