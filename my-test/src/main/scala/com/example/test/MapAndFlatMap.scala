package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MapAndFlatMap extends Logging{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getName).getOrCreate()


    val arrayStructureData = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),"CA"),
      Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
      Row("Robert,,Williams",List("CSharp","VB","R"),"NV")
    )

    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("currentState", StringType)

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    import spark.implicits._

    //flatMap() Usage
    val df2=df.flatMap(f => {
//      val lang=f.getSeq[String](1)
val lang: Seq[String] = f.getSeq[String](1)
      lang.map((f.getString(0),_,f.getString(2)))
    })

    val df3=df2.toDF("Name","language","State")
    df3.show(false)

    spark.stop()
  }
}
