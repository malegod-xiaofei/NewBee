package bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark02_RDD_Persist {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRdd = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRdd.map(word => {
      println("%%%%%%%")
      (word, 1)
    })

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("*******************")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
