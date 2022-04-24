package com.example.test

import org.apache.spark.sql.SparkSession

object Test05 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    import spark.implicits._
    val columns = Seq("name","address")
    val data = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
      ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
    val dfFromData = spark.createDataFrame(data).toDF(columns: _*)
    dfFromData.printSchema()

    val newDF = dfFromData.map(f=>{
      val nameSplit = f.getAs[String](0).split(",")
      val addSplit = f.getAs[String](1).split(",")
      (nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3))
    })
    val finalDF = newDF.toDF("First Name","Last Name",
      "Address Line1","City","State","zipCode")
    finalDF.printSchema()
    finalDF.show(false)

    spark.stop()
  }
}
