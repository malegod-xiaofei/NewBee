package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark17_RDD_Operator_Transform_FoldByKey {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - Key - Value 类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)
      , ("b", 4), ("b", 5), ("a", 6)
    ), 2)


    rdd.aggregateByKey(0)(
      _ + _,
      _ + _
    ).collect().foreach(print)

    // 如果聚合计算时，分区内和分区间计算规则相同时，spark提供了简化的方法
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    sc.stop()

  }

}
