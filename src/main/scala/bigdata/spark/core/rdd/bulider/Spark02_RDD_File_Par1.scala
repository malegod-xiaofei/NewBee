package bigdata.spark.core.rdd.bulider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-01-14:08
 */
object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 创建 RDD
    // TODO 数据分区的分配
    // 1.数据以行为单位进行读取
    //     spark 读取文件，采用的是 hadoop 的方式读取，所以一行一行读取，和字节数没有关系
    // 2.数据读取时以偏移量为单位,偏移量不会被重复读取
    /*
      1@@ => 012
      2@@ => 345
      3   => 6
    */
    // 3.数据分区的偏移量范围的计算
    // 0 =>[0,3] => 12
    // 1 =>[3,6] => 3
    // 2 =>[6,7] =>

    // 【1,2】，【3】，【】

    val rdd: RDD[String] = sc.textFile(Thread.currentThread().getContextClassLoader.getResource("datas/1.txt").getPath, 2)
    rdd.saveAsTextFile("src/main/resources/output")

    // TODO 关闭环境
    sc.stop()
  }
}
