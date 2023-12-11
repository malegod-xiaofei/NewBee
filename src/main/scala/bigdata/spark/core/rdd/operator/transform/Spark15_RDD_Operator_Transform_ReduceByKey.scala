package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark15_RDD_Operator_Transform_ReduceByKey {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - Key - Value 类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    // reduceByKey：相同的 key 的数据进行 value 数据的聚合操作
    // scala 语言中一般的聚合操作都是两两聚合，spark 基于 scala 开发的，所以他的聚合也是两两聚合
    // 【1,2,3】
    // 【3,3】
    // 【6】
    // reduceByKey 中如果 key 的数据只有一个，是不会参与运算的。
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x = ${x},y = ${y}")
      x + y
    })

    reduceRDD.collect().foreach(println)

    sc.stop()

  }

}
