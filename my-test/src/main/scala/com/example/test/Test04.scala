package com.example.test

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
object Test04 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    val data = Seq(Row(Row("James;","","Smith"),"36636","M","3000"),
      Row(Row("Michael","Rose",""),"40288","M","4000"),
      Row(Row("Robert","","Williams"),"42114","M","4000"),
      Row(Row("Maria","Anne","Jones"),"39192","F","4000"),
      Row(Row("Jen","Mary","Brown"),"","F","-1")
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
      .withColumn("Country",lit("USA"))
      .withColumn("anotherColumn",lit("anotherValue"))
      .withColumn("salary",col("salary")*100)
      .withColumn("CopiedColumn",col("salary")* -1)
      .withColumn("salary",col("salary").cast("Integer"))
      .withColumnRenamed("gender","sex")
      .drop("CopiedColumn")
    df.printSchema()
    df.show()
    df.createOrReplaceTempView("PERSON")
    val df2: DataFrame = spark.sql("select * from PERSON")
    df2.show()
    df.select("name").show()
    df.select("name.firstname","name.lastname").show()

    df.select("name.*").show()

    val schema2: StructType = new StructType().add("fname", StringType).add("middlename", StringType).add("lname", StringType)
    df.select(col("name").cast(schema2),
      col("dob"),
      col("sex"),
      col("salary")).printSchema()

    df.select(col("name.firstname").as("fname"),
      col("name.middlename").as("mname"),
      col("name.lastname").as("lname"),
      col("dob")).printSchema()

    spark.stop()
  }
}
