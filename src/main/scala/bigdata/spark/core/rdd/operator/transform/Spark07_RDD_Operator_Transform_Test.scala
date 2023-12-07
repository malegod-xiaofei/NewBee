package bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark07_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - filter
    val rdd: RDD[String] = sc.textFile("src/main/resources/datas/apache.log")
    rdd.filter(line => {
      val datas: Array[String] = line.split(" ")
      val time: String = datas(3)
      time.startsWith("17/05/2015")
    }).collect().foreach(println)

    sc.stop()
  }

}
