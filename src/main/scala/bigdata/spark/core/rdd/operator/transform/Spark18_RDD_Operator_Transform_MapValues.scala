package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark18_RDD_Operator_Transform_MapValues {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - Key - Value 类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3)
      , ("b", 4), ("b", 5), ("a", 6)
    ), 2)


    // aggregateByKey 最终的返回数据结果应该和初始值的类型保持一致
    // val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)
    // aggRDD.collect().foreach(print)

    // 获取相同 key 的数据的平均值 => （a,3),(6,4)
    // 先对各个分区中的数据进行预聚合，分区内操作： a(3,2),b(3,1)  b(9,2),a(6,1)
    // 分区间相加 : a(9,3),b(12,3)
    // 最后求平均数 : a(3),b(4)
    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        println(s"t : ${t}")
        println(s"v : ${v}")
        println(s"t._1 : ${t._1},t._2 : ${t._2} ")
        (t._1 + v, t._2 + 1)
      }, (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    print("newRDD :")
    newRDD.collect().foreach(println)

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
