package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark17_RDD_Operator_Transform_AggregateByKey1 {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - Key - Value 类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)
      , ("b", 4), ("b", 5), ("a", 6)
    ), 2)


    // math.min(x,y)
    // math.max(x,y)
    rdd.aggregateByKey(5)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(print)

    rdd.aggregateByKey(0)(
      (x, y) => x + y,
      (x, y) => x + y
    ).collect().foreach(print)

    rdd.aggregateByKey(0)(
      _ + _,
      _ + _
    ).collect().foreach(print)
    sc.stop()

  }

}
