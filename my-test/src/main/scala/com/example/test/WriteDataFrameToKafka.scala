
package com.example.test

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.{RandomUtils, StringUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
  * 写kafka
  */
object WriteDataFrameToKafka extends Logging{

  def main(args: Array[String]): Unit = {
    if(args.length < 1){
      logError(
        """
          |请输入kafka.bootstrap.servers的值 例如 xxxx:9092
          |""".stripMargin)
      System.exit(1)
    }
    logWarning("参数 = "+args(0))
    val jsonParam: JSONObject = JSON.parseObject(args(0))
    val kafkaBootstrapServers: String = jsonParam.getString("kafka.bootstrap.servers")
    if(StringUtils.isBlank(kafkaBootstrapServers)){
      logError(
        """
          |请输入kafka.bootstrap.servers的值 例如 xxxx:9092
          |""".stripMargin)
      System.exit(1)
    }
    val topic: String = jsonParam.getString("topic")
    if(StringUtils.isBlank(topic)){
      logError(
        """
          |请输入topic的值
          |""".stripMargin)
      System.exit(1)
    }
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    var localDateTime: LocalDateTime = LocalDateTime.of(2022, 5, 9, 5, 0, 0)
    val jsonObject: JSONObject = new JSONObject()
    jsonObject.put("click_url","http://www.baidu.com")
    jsonObject.put("ts",dateTimeFormatter.format(localDateTime))
    var data = Seq (("", jsonObject.toJSONString))
    for(i <-1 until 60){
      var temp: LocalDateTime = localDateTime.plusMinutes(i).plusSeconds(RandomUtils.nextInt(0,59))
      var str: String = dateTimeFormatter.format(temp)
      jsonObject.put("ts",str)
      log.info("数据 = "+jsonObject.toJSONString)
      data = data :+ ("",jsonObject.toJSONString)

      temp = localDateTime.plusMinutes(i).plusSeconds(RandomUtils.nextInt(0,59))
      str = dateTimeFormatter.format(temp)
      jsonObject.put("ts",str)
      log.info("数据 = "+jsonObject.toJSONString)
      data = data :+ ("",jsonObject.toJSONString)
    }

    val df = spark.createDataFrame(data).toDF("key","value")
    /*
      since we are using dataframe which is already in text,
      selectExpr is optional. 
      If the bytes of the Kafka records represent UTF8 strings, 
      we can simply use a cast to convert the binary data 
      into the correct type.

      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    */
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers",kafkaBootstrapServers)
      .option("topic",topic)
      .save()
  }
}
