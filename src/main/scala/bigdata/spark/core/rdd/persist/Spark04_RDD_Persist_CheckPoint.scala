package bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark04_RDD_Persist_CheckPoint {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)
    sc.setCheckpointDir("cp")
    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRdd = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRdd.map(word => {
      println("%%%%%%%")
      (word, 1)
    })

    // checkpoint 需要落盘，需要指定检查点报错路径
    // 检查点路径保存的文件，当作业执行完毕后，不会被删除
    // 一般保存路径都是在分布式存储系统：HDFS
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("*******************")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }

}
