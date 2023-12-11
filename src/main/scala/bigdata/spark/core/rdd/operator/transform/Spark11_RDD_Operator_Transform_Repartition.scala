package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark11_RDD_Operator_Transform_Repartition {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - repartition
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // coalesce 算子可以扩大分区的，但是如果不进行 shuffle 操作，是没有意义，不起作用。
    // 所以如果想要实现扩大分区的效果，需要使用 shuffle 操作
    // spark 提供了一个简化的操作
    // 缩减分区：coalesce，如果想要数据均衡，可以采用 shuffle
    // 扩大分区：repartition,底层代码调用的就是 coalesce ，而且肯定采用 shuffle
    // val coalesceRDD: RDD[Int] = rdd.coalesce(3, true)
    val coalesceRDD: RDD[Int] = rdd.repartition(3)

    coalesceRDD.saveAsTextFile("src/main/resources/output2")

  }

}
