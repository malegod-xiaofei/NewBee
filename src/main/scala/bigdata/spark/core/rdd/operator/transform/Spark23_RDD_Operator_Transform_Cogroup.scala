package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark23_RDD_Operator_Transform_Cogroup {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - (Key - Value 类型)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2)//, ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6), ("c", 7)
    ))

    // cogroup : connet + group (分组连接)
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    cogroupRDD.collect().foreach(println)

    sc.stop()

  }

}
