package bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark01_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRdd = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRdd.map((_, 1))

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("*******************")

    val list1 = List("Hello Scala", "Hello Spark")

    val rdd1 = sc.makeRDD(list1)

    val flatRdd1 = rdd1.flatMap(_.split(" "))

    val mapRDD1: RDD[(String, Int)] = flatRdd1.map((_, 1))

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
