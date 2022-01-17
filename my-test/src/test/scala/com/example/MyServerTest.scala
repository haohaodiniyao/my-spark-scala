package com.example

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class MyServerTest extends FunSuite with SparkSessionProvider {

  val param = new JSONObject()
  var thisSpark: SparkSession = null

  override def beforeAll(): Unit = {
    param.put("key1","value1")
    param.put("key2","value2")
    val sparkBuilder = SparkSession.builder().appName(this.getClass.getName).master("local[2]")
    thisSpark = sparkBuilder.getOrCreate()
    thisSpark.sparkContext.setLogLevel("WARN")
  }

  test("testMyServer") {
    System.err.println(
      """
        |请输入正确的参数
        |1
        |2
        |3""".stripMargin
    )
    MyServer.run(thisSpark,param.toString)
  }

  override protected def afterAll(): Unit = {
    thisSpark.stop()
  }
}
