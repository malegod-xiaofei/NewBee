package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark01_RDD_Operator_Transform_Map {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - map

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    def mapFunction(num: Int): Int = {
      num * 2
    }

    // val mapRDD = rdd.map(mapFunction)
    // val mapRDD = rdd.map((num: Int) => num * 2)
    val mapRDD = rdd.map(_ * 2)

    mapRDD.collect().foreach(println)

    sc.stop()

  }

}
