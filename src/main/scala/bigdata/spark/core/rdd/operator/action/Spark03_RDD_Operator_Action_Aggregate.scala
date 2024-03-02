package bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark03_RDD_Operator_Action_Aggregate {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // TODO - 行动算子

    // 13 + 17 = 30
    // aggregateByKey：初始值只会参与分区内计算
    // aggregate：初始值会参与分区内计算，并且和参与分区间计算
    // val result = rdd.aggregate(10)(_ + _, _ + _)
    val result = rdd.fold(10)(_ + _)
    println(result)

    sc.stop()
  }

}
