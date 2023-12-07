package bigdata.spark.core.rdd.bulider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-01-14:08
 */
object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建 RDD
    // val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    // val rdd = sc.makeRDD(List(1, 2, 3, 4), 3)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 3)

    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("src/main/resources/output")

    // TODO 关闭环境
    sc.stop()
  }
}
