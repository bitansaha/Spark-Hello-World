package com.spark.app

import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark-Test-App").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("README.md")
    val counts = data
      .flatMap(line => line.split(" "))
      .count()

    println("Counts - " + counts)

  }

}
