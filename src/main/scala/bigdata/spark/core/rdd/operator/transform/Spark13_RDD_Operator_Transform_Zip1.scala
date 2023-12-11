package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark13_RDD_Operator_Transform_Zip1 {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - 双 value 类型

    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    // 两个数据源要求分区数量要保持一致
    // Can only zip RDDs with same number of elements in each partition
    // 只能压缩每个分区中具有相同数量元素的rdd吗
    // 两个数据源要求分区中数据数量保持一致
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 2)

    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    sc.stop()

  }

}
