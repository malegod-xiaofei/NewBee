package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark13_RDD_Operator_Transform_Zip {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - 双 value 类型

    // 交集，并集和差集要求两个数据源类型保持一致
    // 拉链操作两个数据源的类型可以不一致

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    val rdd7: RDD[String] = sc.makeRDD(List("3", "4", "5", "6"))

    // 交集：【3,4】
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    // val rdd8: RDD[Int] = rdd1.intersection(rdd7)
    println(rdd3.collect().mkString(","))

    // 并集：【1,2,3,4,3,4,5,6】
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    // 差集：【1,2】
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链：【1-3,2-4,3-5,4-6】
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    val rdd8: RDD[(Int, String)] = rdd1.zip(rdd7)
    println(rdd6.collect().mkString(","))
    println(rdd8.collect().mkString(","))

    sc.stop()

  }

}
