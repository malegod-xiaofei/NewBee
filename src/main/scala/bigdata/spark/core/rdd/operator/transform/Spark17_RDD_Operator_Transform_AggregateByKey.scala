package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark17_RDD_Operator_Transform_AggregateByKey {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - Key - Value 类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    // (a【1,2】),(a【3,4】)
    // (a,2),(b,4)
    // (6)

    // aggregateByKey 存在函数柯里化，有两个参数列表
    // 第一个参数列表
    //    需要传递一个参数，表示为初始值
    //    主要用于当碰见第一个 key 的时候，和 value 进行分区内计算
    // 第二个参数列表需要传递两个参数
    //     第一个参数表示分区内计算规则
    //     第二个参数表示分区间计算规则

    // math.min(x,y)
    // math.max(x,y)
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(print)

    sc.stop()

  }

}
