package bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark07_RDD_Operator_Transform_Filter {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - filter
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD: RDD[Int] = rdd.filter(_ % 2 != 0)

    filterRDD.collect().foreach(println)

    sc.stop

  }

}
