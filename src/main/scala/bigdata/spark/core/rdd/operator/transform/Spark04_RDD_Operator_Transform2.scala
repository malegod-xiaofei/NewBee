package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark04_RDD_Operator_Transform2 {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - mapPartitions
    val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val flatRDD = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )

    flatRDD.collect().foreach(println)

    sc.stop()

  }

}
