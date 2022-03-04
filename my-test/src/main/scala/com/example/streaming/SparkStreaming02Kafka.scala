package com.example.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * nc -lk 9999
 */
object SparkStreaming02Kafka {
  def main(args: Array[String]): Unit = {
    //创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingKafka")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    val kafkaPara = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "test.sz-short-video.sz-item.shareit-cassandra-test.sg2.stream:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "hello_group",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaDataDS = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("whelloworld"), kafkaPara)
    )
    kafkaDataDS.map(_.value()).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
