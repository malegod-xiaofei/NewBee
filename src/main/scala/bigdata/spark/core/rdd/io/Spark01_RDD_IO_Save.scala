package bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark01_RDD_IO_Save {

  def main(args: Array[String]): Unit = {


    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ))
    rdd.saveAsTextFile("src/main/resources/output1")
    rdd.saveAsObjectFile("src/main/resources/output2")
    rdd.saveAsSequenceFile("src/main/resources/output3")

    sc.stop()
  }
}
