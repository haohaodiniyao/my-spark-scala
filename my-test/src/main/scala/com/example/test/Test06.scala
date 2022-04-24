package com.example.test

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.col

object Test06 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()

    val data: Seq[(String, String, String, String)] = Seq(("James", "Smith", "USA", "CA"),
      ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"),
      ("Maria", "Jones", "USA", "FL")
    )
    val columns: Seq[String] = Seq("firstname","lastname","country","state")
    import spark.implicits._
    val df = data.toDF(columns:_*)
    df.printSchema()
    df.select("firstname","lastname").show()
    df.select(df("firstname"),df("lastname")).show()
    df.select(col("firstname").alias("fname"),col("lastname")).show()
    df.select("*").show()
    val columns1: Array[String] = df.columns
    val columns2: Array[Column] = columns1.map(m => col(m))
    df.select(columns2:_*).show()
    df.select(columns.map(m=>col(m)):_*).show()
    val listCols: List[String] = List("lastname", "country")
    df.select(listCols.map(m=>col(m)):_*).show()
    df.select(df.columns.slice(0,3).map(m=>col(m)):_*).show()
    df.select(df.columns(3)).show()
    df.select(df.columns.slice(2,4).map(m=>col(m)):_*).show()

    //Select columns by regular expression
    df.select(df.colRegex("`^.*name*`")).show()

    df.select(df.columns.filter(f=>f.startsWith("first")).map(m=>col(m)):_*)
    df.select(df.columns.filter(f=>f.endsWith("name")).map(m=>col(m)):_*)


    val old_columns = Seq("dob","gender","salary","fname","mname","lname")
    val new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
    val columnsList: Seq[(String, String)] = old_columns.zip(new_columns)
    val columnsList2: Seq[Column] = columnsList.map(f => {
      col(f._1).as(f._2)
    })
    val df5 = df.select(columnsList2:_*)
    df5.printSchema()


    spark.stop()
  }
}
