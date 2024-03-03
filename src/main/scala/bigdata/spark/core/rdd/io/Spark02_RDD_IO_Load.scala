package bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark02_RDD_IO_Load {

  def main(args: Array[String]): Unit = {


    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val rdd = sc.textFile("src/main/resources/output1")
    println(rdd.collect().mkString(","))

    val rdd1 = sc.objectFile[(String, Int)]("src/main/resources/output2")
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.sequenceFile[String, Int]("src/main/resources/output3")
    println(rdd2.collect().mkString(","))

    sc.stop()
  }
}
