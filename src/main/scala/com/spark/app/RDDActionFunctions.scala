package com.spark.app

import com.spark.app.model.Person
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object RDDActionFunctions extends App {

  implicit val sc = JobSession.sparkContext

  //testReduce
  //testFold
  //testAggregate
  testCountByValue

  /**
    * Reduce Action takes a function which takes two arguments of the same type as that of the RDD and returns a single
    * item of the same type of the RDD. Primarily meant to Add, Subtract or some how join all elements of the RDD. Again
    * Reduce can not return a different type from that of the RDD
    * @param sc
    */
  def testReduce (implicit sc: SparkContext): Unit = {
    val rdd = sc.parallelize(Array(1, 2, 3))

    println(rdd.reduce(_ + _))
  }

  /**
    * Fold is a curried function which takes a Zero-Initializer as the first argument and a Reduce function as the second
    * argument. The Zero-Initializer value is used as the Initializer for the Reduce function for every Partition
    * separately, and as an Initializer to Reduce the reduced output of all Partitions.
    * @param sc
    */
  def testFold (implicit sc: SparkContext): Unit = {
    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5), 3)

    println(rdd.fold(5)(_ + _))
    // (5 + 1) + (5 + 2 + 3) + (5 + 4 + 5) + 5 = 35
    // [ZI + SUM(Partition_1)] + [ZI + SUM(Partition_2)] + [ZI + SUM(Partition_3)] + ZI = 35
    // Note: Zero-Initializer are meant to be a neutral element which does not effect the output of the reducer, in the
    // above case it should have been 0, but 5 was used to depict the functionality.

    /**
      * Fold Vs Reduce
      */

    // Fold a better choice if there is a possibility of the RDD being empty
    val emptyArray: Array[Int] = Array()
    val emptyRdd: RDD[Int] = sc.parallelize(emptyArray)

    try {
      emptyRdd.reduce(_ + _)
    } catch {
      case _ => println("Reducing an empty RDD threw an exception")
    }

    println("Empty RDD reduced value - " + emptyRdd.fold(0)(_ + _))

    // Fold a better choice if call to the Reduce function return's a new instance of the reduced RDD so far. With Fold
    // we can use an immutable instance to hold the reduced values for every call to the reduced function.

    val array1: Array[Int] = Array(1, 2, 3)
    val array2: Array[Int] = Array(4, 5, 6)
    val array2D: Array[Array[Int]] = Array(array1, array2)

    val arrayRdd = sc.parallelize(array2D)

    // Reducing an RDD of 2D Array's, each call to the Reduce function creates a new Array to hold the reduced values so
    // far. Triggering the creation and garbage collection of N arrays each of n dimensions.
    arrayRdd.reduce((a1, a2) => a1.zip(a2).map(values => values._1 + values._2)).foreach(println)

    // Here we create an array containing all Zero's and use it as Zero-Initializer, and always keep updating the
    // Zero-Initializer array for every call to the reduce function with the reduced values so far, there by restricting
    // creation of extra object as was in the the previous case.
    val emptyImmutableArray = Array(0, 0, 0)
    arrayRdd.fold(emptyImmutableArray)((a1, a2) => {
      for (index <- a1.indices) a1(index) = a1(index) + a2(index)
      a1
    }).foreach(println)

  }

  /**
    * Aggregate does not dictate that the return type has to be the same as that of the RDD. Like Fold it is a curried
    * function which takes a Zero-Initializer as the first argument and a pair of functions (F1, F2) as the second
    * argument.
    * F1: Function 1 is meant to reduce an RDD element into the Zero-Initializer
    * F2: Function 2 is meant to reduce TWO Zero-Initializer into ONE
    * @param sc
    */
  def testAggregate(implicit sc: SparkContext): Unit = {
    val data = 1 to 100 toArray
    val rdd = sc.parallelize(data)
    // A tuple as the zero-initializer to hold the SUM in index-0 and MAX in index-1
    val zeroInitializer = (0, 0)

    // Using aggregate to reduce the given RDD to obtain the SUM and MAX values
    val sumMaxValue = rdd.aggregate(zeroInitializer)(
      (zi, value) => (zi._1 + value, if (zi._2 > value) zi._2 else value),
      (zi1, zi2) => (zi1._1 + zi2._1, if (zi1._2 > zi2._2) zi1._2 else zi2._2)
    )

    println("Sum - " + sumMaxValue._1)
    println("Max - " + sumMaxValue._2)
  }

  /**
    * CountByValue reduces a given RDD to Pairs of Unique RDD elements and their Counts
    * @param sc
    */
  def testCountByValue(implicit sc: SparkContext): Unit = {
    val personArray = Array(Person("A"), Person("B"), Person("A"))
    val rdd = sc.parallelize(personArray)
    
    rdd.countByValue().foreach { case (person: Person, count: Long) => println("Person - " + person + ", Count - " + count) }
  }

}
