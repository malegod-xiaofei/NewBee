package bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark02_RDD_Operator_Action_Reduce {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子

    // val i: Int = rdd.reduce(_ + _)
    // println(i)

    // collect : 方法会将不同分区的数据按照分区顺序采集到 Driver 端内存中，形成数组
    // val ints: Array[Int] = rdd.collect()
    // println(ints.mkString(","))

    // count : 数据源中数据的个数
    val cnt: Long = rdd.count()
    println(cnt)

    // first : 获取数据源中数据的第一个
    val first: Int = rdd.first()
    println(first)

    // take : 获取 N 个数据
    val ints: Array[Int] = rdd.take(3)
    println(ints.mkString(","))

    // takeOrdered : 数据排序后，取 N 个数据
    val rdd1: RDD[Int] = sc.makeRDD(List(4, 2, 3, 1))
    val ints1: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse)
    println(ints1.mkString(","))

    sc.stop()
  }

}
