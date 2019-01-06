package com.spark.app

import org.apache.spark.{SparkConf, SparkContext}

object JobSession {

  lazy val sparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("Spark-Test-App").setMaster("local[2]")
    new SparkContext(conf)
  }
}
