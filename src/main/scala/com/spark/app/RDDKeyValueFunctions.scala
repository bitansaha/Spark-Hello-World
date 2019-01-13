package com.spark.app

import com.spark.app.model.Person
import org.apache.spark.SparkContext

object RDDKeyValueFunctions extends App {

  implicit val sc = JobSession.sparkContext
  //testReduceByKey
  //testFoldByKey
  //testCombineByKey
  //testReduceByWithParallelism
  //testGroupByKey
  //testCoGroup
  //testInnerJoin
  //testLeftOuterJoin
  //testRightOuterJoin
  //testFullOuterJoin
  testSortByKey

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
    * All _ByKey functions has an overloaded method which takes 'numPartitions' as an input and shuffles all locally
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

  /**
    * The primary difference between groupByKey and reduce/fold/combineByKey is the fact that in groupByKey all Key Value
    * pairs across all partitions for a given Key are shuffled to a Spark/System provided partition and the resultant
    * RDD contains the Key and an Iterable with all collective Values for a given Key. Where as reduceByKey first reduces
    * all Values for a given Key locally and then shuffles the locally reduced Values of each Key to a Spark/System
    * provided partition, hence clearly network bandwidth required for groupByKey is far larger than reduceByKey for the
    * purpose of shuffling. In-case if we want to group and reduce we should never use groupByKey and then reduce the
    * iterable rather we should use reduceByKey or some variant of it
    * @param sc
    */
  def testGroupByKey (implicit sc: SparkContext) : Unit = {
    val data = Array(("A", 1), ("B", 1), ("A", 1), ("A", 2), ("B", 3))

    // key/value paired RDD
    val rdd = sc.parallelize(data, 4)

    rdd.groupByKey().map(_ match {case (key, values) => (key, values.sum)}).collect().foreach(println)
  }

  /**
    * CoGroup performs a groupByKey over multiple RDD's with similar Key type resulting in a Key and Iterable of Values
    * for every coGrouped RDD for a given Key. Hence the output is Key with a Tuple of the Iterable of Values of each
    * coGrouped RDD. (Key, (Iterable[RDD1_Values], Iterable[RDD2_Values]))
    * coGroup can also be thought about as full join for multiple RDD's
    * @param sc
    */
  def testCoGroup (implicit sc: SparkContext) : Unit = {
    val data1 = Array(("A", 1), ("B", 1), ("A", 1), ("A", 2), ("B", 3))
    val data2 = Array(("A", "ABC"), ("B", "DEF"), ("A", "GHI"), ("A", "JKL"), ("B", "MNO"))
    val data3 = Array(("A", true), ("B", false), ("A", true), ("A", false), ("B", true))

    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)
    val rdd3 = sc.parallelize(data3)

    rdd1.cogroup(rdd2).cogroup(rdd3).collect().foreach(println)
  }

  /**
    * All Joins uses coGroup internally to join not multiple but two RDD's with similar Key types. The output is the Key
    * and a Tuple of both the values from both the RDD's. For inner-join any Key which does not exists in both the RDD's
    * is dropped from the resultant RDD. In case there are more than One Value for a given Key in either or both of the
    * RDD's then this results in multiple rows in the output RDD with the same Key and Value being a pair between a value
    * from RDD_1 and RDD_2.
    * All Joins trigger a shuffle across all partitions and hence are quite expensive.
    * @param sc
    */
  def testInnerJoin (implicit sc: SparkContext) : Unit = {
    val data1 = Array(("A", 1), ("B", 1), ("A", 2), ("C", 4))
    val data2 = Array(("A", true), ("B", false), ("A", false), ("D", false))

    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)

    rdd1.join(rdd2).collect().foreach(println)
  }

  /**
    * Retains common join properties from above. Shows all Key and Values from Left RDD irrespective of there being a match
    * available in right RDD. The output is Key and Pair of Value from Left RDD and Optional Value from Right RDD (as
    * right RDD value might not be available)
    * @param sc
    */
  def testLeftOuterJoin (implicit sc: SparkContext) : Unit = {
    val data1 = Array(("A", 1), ("B", 1), ("A", 2), ("C", 4))
    val data2 = Array(("A", true), ("B", false), ("A", false), ("D", false))

    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)

    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
  }

  /**
    * Retains common join properties from above. Shows all Key and Values from Right RDD irrespective of there being a match
    * available in Left RDD. The output is Key and Pair of  Optional Value from Left RDD and Value from Right RDD
    * @param sc
    */
  def testRightOuterJoin (implicit sc: SparkContext) : Unit = {
    val data1 = Array(("A", 1), ("B", 1), ("A", 2), ("C", 4))
    val data2 = Array(("A", true), ("B", false), ("A", false), ("D", false))

    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)

    rdd1.rightOuterJoin(rdd2).collect().foreach(println)
  }

  /**
    * Retains common join properties from above. Shows all Key and Values from Right and Left RDD's irrespective of there
    * being a match available in the other RDD. The output is Key and Pair of Optional Value from Left RDD and Optional
    * Value from Right RDD
    * @param sc
    */
  def testFullOuterJoin (implicit sc: SparkContext) : Unit = {
    val data1 = Array(("A", 1), ("B", 1), ("A", 2), ("C", 4))
    val data2 = Array(("A", true), ("B", false), ("A", false), ("D", false))

    val rdd1 = sc.parallelize(data1)
    val rdd2 = sc.parallelize(data2)

    rdd1.fullOuterJoin(rdd2).collect().foreach(println)
  }

  /**
    * sortByKey takes a flag for ascending/descending (and a non-compulsory 'number of partitions') and Sort's the RDD
    * by the Key and an implicit Comparator to compare complex Key types.
    *
    * sortBy works with non Key-Value pair RDD's and takes a Function to pin point which attributes should be used as the
    * Key to perform the Sort, a flag for ascending/descending
    * @param sc
    */
  def testSortByKey (implicit sc: SparkContext) : Unit = {
    // Sorting Key Value pair RDD
    val data1 = Array(("A", 1), ("B", 1), ("A", 2), ("C", 4))

    val rdd1 = sc.parallelize(data1)

    rdd1.sortByKey(true).collect().foreach(println)

    // Sorting Key Value pair RDD with complex Key hence the Comparator
    val data2 = Array((("A", 1), Person("A", 10)), (("B", 1), Person("B", 10)), (("A", 2), Person("C", 10)), (("C", 4), Person("D", 10)))

    val rdd2 = sc.parallelize(data2)

    implicit val personComparator = new Ordering[Tuple2[String, Int]] {
      override def compare(pair1: (String, Int), pair2: (String, Int)): Int = {
        (pair1, pair2) match {
          case ((pair1Key, pair1Value), (pair2Key, pair2Value)) =>
            if (pair1Key.compareTo(pair2Key) == 0) pair1Value.compareTo(pair2Value) else pair1Key.compareTo(pair2Key)
        }
      }
    }

    rdd2.sortByKey().collect().foreach(println)

    // Sorting non Key Value pair RDD hence the function to pinpoint the attribute's meant to behave as the Key
    val data3 = Array(Person("A", 10), Person("A", 20), Person("B", 40), Person("C", 20))

    val rdd3 = sc.parallelize(data3)

    rdd3.sortBy(_.name, true).collect().foreach(println)
    rdd3.sortBy(_.name, false).collect().foreach(println)

    rdd3.sortBy(_.age, true).collect().foreach(println)
    rdd3.sortBy(_.age, false).collect().foreach(println)
  }

}
