package com.example

import org.apache.hadoop.fs.LocalFileSystem
import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}

/**
 * spark session provider for test
 */
trait SparkSessionProvider extends BeforeAndAfterAll {
  self: Suite =>
  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config("spark.driver.memory", "50m")
      .config("spark.executor.memory", "50m")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.hadoop.fs.file.impl", classOf[LocalFileSystem].getName)
      .config("spark.testing", "")   //to allow small executor memory setting for testing
      .getOrCreate()

    sc = spark.sparkContext
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")// rebind issue
  }

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }
}