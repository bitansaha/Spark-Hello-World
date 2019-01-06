package com.spark.app

import com.spark.app.model.Person
import org.apache.spark.SparkContext

object RDDFunctions extends App {
  implicit val sc = JobSession.sparkContext
  //testUnion
  //testIntersection
  //testDistinct
  //testSubtract
  //testCartesian
  testSample

  /**
    * Union doesn't have any performance issue although it will have duplicates if both the RDD's contain the same
    * element. In the below case there will be two 3's as a result of rdd1.union(rdd2)
    * @param sc
    */
  def testUnion (implicit sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(Array(1, 2, 3))
    val rdd2 = sc.parallelize(Array(3, 4, 5, 6))
    val rdd3 = sc.parallelize(Array(7, 8, 9))

    println(rdd1.union(rdd2).union(rdd3).count())

  }

  /**
    * Intersection is an expensive operation as it requires shuffling across all partitions, but unlike Union it removes
    * duplicates.
    * @param sc
    */
  def testIntersection(implicit sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(Array(1, 2, 3, 3, 4, 5))
    val rdd2 = sc.parallelize(Array(3, 4, 6))

    rdd1.intersection(rdd2).collect().foreach(println)
  }

  /**
    * Distinct is expensive as it requires shuffling across all partitions to pick out the duplicates
    * @param sc
    */
  def testDistinct(implicit sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(Array(1, 2, 3, 2, 4))

    rdd1.distinct().collect().foreach(println)

    val personRdd = sc.parallelize(Array(Person("A"), Person("B"), Person("A")))
    personRdd.distinct().collect().foreach(println)
  }

  /**
    * Subtract removes the common elements between RDD_1 and RDD_2 from RDD_1 and return's a new RDD with the remaining
    * elements of RDD_1. It's again an expensive operation as shuffling is required across all partitions to point out
    * intersecting elements.
    * @param sc
    */
  def testSubtract(implicit sc: SparkContext): Unit = {
    val array1 = 1 to 20 toArray
    val array2 = 1 to 10 toArray
    val rdd1 = sc.parallelize(array1)
    val rdd2 = sc.parallelize(array2)

    rdd1.subtract(rdd2).collect().foreach(println)
  }

  /**
    * Cartesian creates a pair of each element from RDD_1 with an element from RDD_2, again an expensive operation as
    * shuffling is required across all partitions of both the RDD's
    * @param sc
    */
  def testCartesian(implicit sc: SparkContext): Unit = {
    val rdd1 = sc.parallelize(Array(1, 2, 3, 4))
    val rdd2 = sc.parallelize(Array('a', 'b', 'c'))

    rdd1.cartesian(rdd2).collect().foreach(pair => println("Int Value - " + pair._1 + ", Char Value - " + pair._2))
  }

  /**
    * Sample is used to get a sample of a given RDD
    * @param sc
    */
  def testSample(implicit sc: SparkContext): Unit = {
    val data = 1 to 100 toArray
    val rdd = sc.parallelize(data)

    // creating a sample size that is half the size of the original data, with out repeating one element twice
    rdd.sample(false, 0.5).collect().foreach(println)
  }

}
