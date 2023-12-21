package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark22_RDD_Operator_Transform_LeftJoin {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - (Key - Value 类型)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6)
    ))

    val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    val rightJoinRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)

    leftJoinRDD.collect().foreach(println)
    println("==========")
    rightJoinRDD.collect().foreach(println)

    sc.stop()

  }

}
