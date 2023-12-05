package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark03_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 【1,2】,【3,4】
    // 【2】,【4】
    val mapRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else
          Nil.iterator
      }
    )
    mapRDD.collect().foreach(println)

    sc.stop()

  }

}
