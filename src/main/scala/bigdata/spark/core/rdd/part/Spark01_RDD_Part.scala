package bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark01_RDD_Part {

  def main(args: Array[String]): Unit = {


    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxx"),
      ("cba", "xxxxxxx"),
      ("wba", "xxxxxxx"),
      ("qba", "xxxxxxx"),
      ("aba", "xxxxxxx"),
      ("zba", "xxxxxxx")
    ), 3)
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new myPartitioner)
    partRDD.saveAsTextFile("src/main/resources/output4")
    sc.stop()
  }

  /**
   * 自定义分区器
   * 1.继承 Partitioner
   * 2.重写方法
   */
  class myPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的 key 值返回数据的分区索引(从0开始)
    override def getPartition(key: Any): Int = {

      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }

    }
  }

}
