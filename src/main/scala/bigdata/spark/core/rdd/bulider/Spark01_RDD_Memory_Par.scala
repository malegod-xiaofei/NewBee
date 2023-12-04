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
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(sparkConf)

    // TODO 创建 RDD
    // RDD 的并行度 & 分区
    // makeRDD 方法可以传递第二个参数，这个参数表示分区的数量
    // 第二个参数可以不传递，那么 makeRDD 方法会使用默认值 ： defaultParallelism (默认的并行度)
    // scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // spark 在默认情况下，从配置对象中获取配置参数：spark.default.parallelism
    // 如果获取不到，那么使用 totalCores属性，这个属性取值为当前运行环境的最大可用核心数
    // val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 将处理的数据保存成分区文件
    rdd.saveAsTextFile("src/main/resources/output")

    // TODO 关闭环境
    sc.stop()
  }
}
