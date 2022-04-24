package com.example.test

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, udf}

/**
  * 单词首字母大写
  */
object Test09 {

  val convertCase =  (strQuote:String) => {
    if(StringUtils.isNotBlank(strQuote)){
      val arr = strQuote.split(" ")
      val strings: Array[String] = arr.map(f => f.substring(0, 1).toUpperCase + f.substring(1, f.length))
      strings.mkString(" ")
    }else{
      strQuote
    }
  }

  val convertUDF = udf(convertCase)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getName).getOrCreate()


    import spark.implicits._
    val columns = Seq("Seqno","Quote")
    val data = Seq(("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "")
    )
    val df = data.toDF(columns:_*)
    df.show(false)
    df.select(col("Seqno"),convertUDF(col("Quote")).as("Quote")).show(false)

    //Using it on SQL
    spark.udf.register("convertUDF",convertCase)
    df.createOrReplaceTempView("QUOTE_TABLE")
    spark.sql("select Seqno,convertUDF(Quote) from QUOTE_TABLE")

    spark.stop()
  }
}
