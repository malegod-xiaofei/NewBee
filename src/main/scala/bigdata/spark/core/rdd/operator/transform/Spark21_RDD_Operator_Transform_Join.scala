package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark21_RDD_Operator_Transform_Join {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - (Key - Value 类型)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 5), ("c", 6), ("a", 4)))

    // join : 两个不同数据源的数据，相同的 key 和 value 会连接在一起，形成元组
    // 如果两个数据源中的 key 没有匹配上，那么数据不会出现在结果中
    // 如果两个数据源中的 key 有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据会几何性增长，会导致性能降低。
    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    joinRDD.collect().foreach(println)

    sc.stop()

  }

}
