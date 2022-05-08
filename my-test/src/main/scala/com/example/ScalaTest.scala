package com.example

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object ScalaTest {
  def main(args: Array[String]): Unit = {
    val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    var localDateTime: LocalDateTime = LocalDateTime.of(2022, 5, 7, 16, 0, 0)
    for(i <-1 until 60){
      localDateTime = localDateTime.plusMinutes(1)
      val str: String = dateTimeFormatter.format(localDateTime)
      System.out.println(i + " = " + str)
    }
  }
}
