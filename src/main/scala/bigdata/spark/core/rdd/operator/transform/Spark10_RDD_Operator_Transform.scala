package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark10_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - coalesce
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // coalesce方法默认情况下不会将分区的数据打乱重新组合
    val coalesceRDD = rdd.coalesce(2)


    coalesceRDD.saveAsTextFile("src/main/resources/output2")

  }

}
