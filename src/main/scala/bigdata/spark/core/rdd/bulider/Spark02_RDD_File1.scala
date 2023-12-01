package bigdata.spark.core.rdd.bulider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-01-14:08
 */
object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 创建 RDD
    // 从文件中创建 RDD，将文件中的数据作为处理的数据源
    // textFile : 以行为单位读取文件
    // wholeTextFiles : 以文件为单位读取数据
    // 读取的结果表示为元组，第一个元素表示文件路径，第二个元素表示文件内容
    val rdd: RDD[(String, String)] = sc.wholeTextFiles(Thread.currentThread().getContextClassLoader.getResource("datas/1.txt").getPath)

    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
