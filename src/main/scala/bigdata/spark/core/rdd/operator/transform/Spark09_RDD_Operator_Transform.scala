package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark09_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - distinct
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))

    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)

    // (1,null),(2,null),(3,null),(4,null),(1,null),(2,null),(3,null),(4,null)
    // (1,null)(1,null)(1,null)
    // (null,null) => null
    val rdd1: RDD[Int] = rdd.distinct()



    rdd1.collect().foreach(println)

  }

}
