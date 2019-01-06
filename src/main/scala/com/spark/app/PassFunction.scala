package com.spark.app

import org.apache.spark.rdd.RDD

/**
  * Depicts the right way to use OO with Spark
  */
class PassFunction {

  private val threshHold = 10

  def serTest(rdd: RDD[Int]): Unit = {

    try {
      rdd.filter(filterValue)
    } catch {
      case _ => println("Can not pass a method of a class directly as function for RDD transformation with out the Class being Serializable")
    }

    println("Total Filtered Items - " + rdd.filter(getFilterValuesFunc).count())
    println("'getFilterValuesFunc' works as it returns a function which in turn is not attached to the Class also creates a Closure containing the threshHold value which otherwise is a Class member")

    implicit val th: Int = threshHold
    println("Total Filtered Items - " + rdd.filter(getFilterValuesFuncWithImplicitTH).count())
    println("'getFilterValuesFuncWithImplicitTH' same as above difference is that the Caller creates a Closure containing the implicit threshHold value which otherwise is a Class member")
  }

  def filterValue (value: Int): Boolean = {
    value < threshHold
  }

  def getFilterValuesFunc: Int => Boolean = {
    val th = threshHold
    (value: Int) => value < th
  }

  def getFilterValuesFuncWithImplicitTH (implicit threshHold: Int): Int => Boolean = {
    (value: Int) => value < threshHold
  }
}

object PassFunction extends App {
  val sc = JobSession.sparkContext
  val data: Array[Int] = 1 to 100 toArray
  val rdd = sc.parallelize(data)
  val passFunction = new PassFunction

  passFunction.serTest(rdd)
}
