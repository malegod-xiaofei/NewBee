package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark01_RDD_Operator_Transform_Part {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - map

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    rdd.saveAsTextFile("src/main/resources/output")

    // 【1,2】，【3,4】
    // 【2,4】，【6,8】
    val mapRDD: RDD[Int] = rdd.map(_ * 2)

    mapRDD.saveAsTextFile("src/main/resources/output1")


    sc.stop()

  }

}
