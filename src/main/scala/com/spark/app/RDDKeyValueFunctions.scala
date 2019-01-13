package com.spark.app

import org.apache.spark.SparkContext

object RDDKeyValueFunctions extends App {

  implicit val sc = JobSession.sparkContext
  //testReduceByKey
  //testFoldByKey
  //testCombineByKey
  testReduceByWithParallelism

  /**
    * The inner working of 'reduceByKey' and it's variants 'foldByKey' and 'combineByKey' can be depicted as follows:
    * 1. All of the above function's work on a key/value pair/tuple.
    * 2. All of the above function's are Transformations and hence returns an RDD as an output.
    * 3. All of the above function's reduces for a given key first with in the local partition.
    * 4. Reduced values for each key in each local partition is then shuffled such that all reduced values of a given
    * key across all local partitions accumulates in a Spark/System provided partition.
    * 5. All locally reduced values for a given key (now co-located) are then merged together to obtain the final
    * reduced value for that key.
    */

  /**
    * ReduceByKey's working remains the same as mentioned above. The output RDD type of the reduceByKey's transformation
    * remains the same as the input RDD (in the below example it is Tuple2(String, Int)) the value's for a given key just
    * gets reduced to a single value.
    * @param sc
    */
  def testReduceByKey (implicit sc: SparkContext) : Unit = {
    val data = Array(("A", 1), ("B", 1), ("A", 1), ("A", 2), ("B", 3))

    // key/value paired RDD
    val rdd = sc.parallelize(data)

    rdd.reduceByKey(_ + _).collect().foreach(println)
  }

  /**
    * FoldByKey's internal working remains the same as mentioned above. FoldByKey is a curried function which takes a
    * Zero-Initializer as the first argument and a Reduce function as the second argument. The Zero-Initializer value is
    * used as the Initializer for the Reduce function for every Key across every Partition. The Zero-Initializer should
    * be a value which does not effect the overall outcome.
    * @param sc
    */
  def testFoldByKey (implicit sc: SparkContext) : Unit = {
    val data = Array(("A", 1), ("B", 1), ("A", 1), ("A", 2), ("B", 3))

    // key/value paired RDD
    val rdd = sc.parallelize(data, 4)

    rdd.foldByKey(5)(_ + _).collect().foreach(println)

    // Initial Stage across all partitions
    // Partition_1 : (A, 1), (B, 1)
    // Partition_2 : (A, 1)
    // Partition_3 : (A, 2), (B, 3)

    // Reduce step at Local partitions. The Zero-Initializer value is used 3 times to initialize the local reducer for A
    // in 3 different partitions.
    // Partition_1 : (A, 5 + 1), (B, 5 + 1)         => (A, 6), (B, 6)
    // Partition_2 : (A, 5 + 1)                     => (A, 6)
    // Partition_3 : (A, 5 + 2), (B, 5 + 3)         => (A, 7), (B, 8)

    // Shuffle similar key's to a single partition and reduce further for each key
    // Partition_1 : (A, 6), (A, 6), (A, 7)         => (A, 19)
    // Partition_2 : (B, 6), (B, 8)                 => (B, 14)

    // Note: Zero-Initializer are meant to be a neutral element which does not effect the output of the reducer, in the
    // above case it should have been 0, but 5 was used to depict the functionality.
  }

  /**
    * CombineByKey does not dictate that the return type has to be the same as that of the Value of the <Key, Value> pair.
    * CombineByKey takes three functions (F1, F2, F3) as compulsory arguments.
    * F1 : This is the Zero-Initializer function called for every Key across all Partitions with the first Value for a
    * Key in it's local Partition as the input. The output is of the type that is intended to be the output of this
    * transformation initialized with the first value provided.
    * F2 : Local merge function which gets the accumulated value so far, of the type that was initialized by the
    * Zero-Initializer function and the current instance of the Value to merged into the Accumulator for a given Key.
    * F3 : Reducer function which can reduce the Accumulator's across all partition to a single Value or merge all
    * accumulator's
    * @param sc
    */
  def testCombineByKey (implicit sc: SparkContext) : Unit = {
    val data = Array(("A", 1), ("B", 1), ("A", 1), ("A", 2), ("B", 3))

    // key/value paired RDD
    val rdd = sc.parallelize(data, 4)

    // The input RDD is of type RDD[String, Int], the output RDD will be of type RDD[String, (Int, Int)]. Where the output
    // Value (tuple) contains the SUM and COUNT for a given key.
    rdd.combineByKey(
      (_, 1),
      (acc: (Int, Int), currentValue: Int) => acc match {
        case (sum, count) => (sum + currentValue, count + 1)
      },
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1, acc2) match {
        case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2)
      }
    ).collect().foreach(println)

  }

  /**
    * All reduceBY_ functions has an overloaded method which takes 'numPartitions' as an input and shuffles all locally
    * reduced key value pairs into that many partitions thereby either reducing or increasing the degree of parallelism
    * beyond this stage.
    * @param sc
    */
  def testReduceByWithParallelism (implicit sc: SparkContext) : Unit = {
    val data = Array(("A", 1), ("B", 1), ("A", 1), ("A", 2), ("B", 3))

    // key/value paired RDD
    val rdd = sc.parallelize(data, 4)

    println("Initial number of partitions - " + rdd.getNumPartitions)

    println("Number of partitions after reduce call - " + rdd.reduceByKey(_ + _, 2).getNumPartitions)
  }
}
