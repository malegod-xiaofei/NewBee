package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark11_RDD_Operator_Transform_SortBy {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(6, 2,  4, 5, 3,1), 2)

    val sortByRDD: RDD[Int] = rdd.sortBy(num => num)

    sortByRDD.saveAsTextFile("src/main/resources/output2")

    sc.stop()

  }

}
