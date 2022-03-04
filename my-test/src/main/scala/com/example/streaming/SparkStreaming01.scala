package com.example.streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * nc -lk 9999
 */
object SparkStreaming01 {
  def main(args: Array[String]): Unit = {
    //创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val word2One = words.map((_, 1))
    val word2Count: DStream[(String,Int)] = word2One.reduceByKey(_ + _)
    word2Count.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
