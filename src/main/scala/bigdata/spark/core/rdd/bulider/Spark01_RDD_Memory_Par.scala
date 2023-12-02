package bigdata.spark.core.rdd.bulider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-01-14:08
 */
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 创建 RDD
    // RDD 的并行度 & 分区


    // TODO 关闭环境
    sc.stop()
  }
}
