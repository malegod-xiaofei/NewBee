package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark19_RDD_Operator_Transform_CombineByKey {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - Key - Value 类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)
      , ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    // combineByKey : 方法需要三个参数
    // 第一个参数表示：将相同 key 的第一个数据进行结构的转换，实现操作
    // 第二个参数表示：分区内的计算规则
    // 第三个参数表示：分区间的计算规则
    val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        println(s"t : ${t}")
        println(s"v : ${v}")
        println(s"t._1 : ${t._1},t._2 : ${t._2} ")
        (t._1 + v, t._2 + 1)
      }, (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    print("newRDD :")
    newRDD.collect().foreach(println)

    // mapValues : key 不变，只对 value 进行操作
    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }
    print("resultRDD :")
    resultRDD.collect().foreach(println)

    sc.stop()

  }

}
