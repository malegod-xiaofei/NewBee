package bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark01_RDD_Dep {

  def main(args: Array[String]): Unit = {

    // Application
    // Spark框架
    // TODO 建立和Spark框架的连接
    // JDBC : Connection
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    // TODO 执行业务操作

    val lines: RDD[String] = sc.textFile(Thread.currentThread().getContextClassLoader.getResource("datas/word.txt").getPath)
    // 打印血缘关系
    println(lines.toDebugString)
    println("********")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("********")

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println("********")

    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToSum.toDebugString)
    println("********")

    val array: Array[(String, Int)] = wordToSum.collect()
    array.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }

}
