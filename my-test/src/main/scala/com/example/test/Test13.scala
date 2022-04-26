package com.example.test

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Test13 extends Logging{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()

    val longAcc: LongAccumulator = spark.sparkContext.longAccumulator("SumAccumulator")
    import spark.implicits._
    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.foreach(x=> longAcc.add(x.getAs[Int]("salary")))
    logWarning("sum = "+longAcc.value)
    spark.stop()
  }
}
