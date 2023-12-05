package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark06_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - group by
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // groupBy 会将数据源中每一个数据进行分组判断，根据返回的分组 key 进行分组
    // 相同的 key 值的数据会防止在一个组中
    def groupFunction(num: Int): Int = {
      num % 2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)

    groupRDD.collect().foreach(println)

    sc.stop()

  }

}
