package bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark04_RDD_Operator_Action_CountByKey {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 1, 1, 4), 2)

    // TODO - 行动算子

    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong)

    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)))
    val stringToLong: collection.Map[String, Long] = rdd1.countByKey()
    println(stringToLong)


    sc.stop()
  }

}
