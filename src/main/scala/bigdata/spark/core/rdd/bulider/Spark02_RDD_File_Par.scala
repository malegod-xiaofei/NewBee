package bigdata.spark.core.rdd.bulider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-01-14:08
 */
object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkconf)

    // TODO 创建 RDD
    // textFile 可以将文件作为数据处理的数据源，默认也可以设定分区。
    //     minPartitions : 最小分区数量
    //     math.min(defaultParallelism, 2)
    // val rdd = sc.textFile(Thread.currentThread().getContextClassLoader.getResource("datas/1.txt").getPath)
    // 如果不想使用默认的分区数量，可以通过第二个参数指定分区数
    // 分区数量的计算方式 ：
    //     totalSize = 7
    //     goalSize = 7 / 2 = 3(byte)

    //     7 / 3 = 2...1 (1.1) + 1 = 3 (分区) 剩余的数占每个分区的数的多少，如果大于了 10%，就等同于产生新分区
    val rdd: RDD[String] = sc.textFile(Thread.currentThread().getContextClassLoader.getResource("datas/1.txt").getPath, 2)
    rdd.saveAsTextFile("src/main/resources/output")

    // TODO 关闭环境
    sc.stop()
  }
}
