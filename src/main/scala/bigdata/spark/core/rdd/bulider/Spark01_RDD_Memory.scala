package bigdata.spark.core.rdd.bulider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-01-14:08
 */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 创建 RDD
    // 从内存中创建 RDD，将内存中集合的数据作为处理的数据源
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4, 5)

    // parallelize : 并行
    // val rdd: RDD[Int] = sc.parallelize(seq)
    // makeRDD 方法在底层实现时其实就是调用了 rdd 对象的 parallelize 方法
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
