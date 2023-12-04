package bigdata.spark.core.rdd.bulider

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-01-14:08
 */
object Spark03_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 创建 RDD

    // 14byte / 2  7byte
    // 14 / 7 = 2 (分区)

    // 偏移量
    /*
    1234567@@ => 012345678
    89@@      => 9101112
    0         => 13

    [0,7]  => 1234567
    [7,14] => 890
     */

    // 如果数据源为多个文件，那么计算分区时以文件为单位进行分区

    val rdd = sc.textFile(Thread.currentThread().getContextClassLoader.getResource("datas/word.txt").getPath, 2)
    rdd.saveAsTextFile("src/main/resources/output")

    // TODO 关闭环境
    sc.stop()
  }
}
