package com.example.test.csv

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object WriteCSV extends Logging {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logError(
        """
          |请输入写入的csv路径
          |""".stripMargin)
      System.exit(1)
    }
    logWarning("参数 = " + args(0))
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()
    val data = Seq(("James", "Smith", "USA", "CA"), ("Michael", "Rose", "USA", "NY"),
      ("Robert", "Williams", "USA", "CA"), ("Maria", "Jones", "USA", "FL")
    )
    val columns = Seq("firstname", "lastname", "country", "state")
    import spark.implicits._
    val df = data.toDF(columns: _*)
    df.write.option("header", true).csv(args(0))

    spark.stop()
  }
}
