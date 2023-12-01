package bigdata.spark.core.rdd.bulider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-01-14:08
 */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 创建 RDD
    // 从文件中创建 RDD，将文件中的数据作为处理的数据源
    var rdd: RDD[String] = sc.textFile(Thread.currentThread().getContextClassLoader.getResource("datas").getPath)
    // path 路径可以是文件的具体路径，也可以是目录的名称
    rdd = sc.textFile("datas")
    // path 路径还可以使用通配符
    rdd = sc.textFile("datas/1*.txt")
    // path 还可以是分布式路径
    rdd = sc.textFile("hdfs://linux1:8020//test.txt")
    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
