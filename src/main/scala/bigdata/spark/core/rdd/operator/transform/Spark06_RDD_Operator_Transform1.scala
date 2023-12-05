package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark06_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - group by
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"), 2)

    // 分组和分区没有必然的关系
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)

    sc.stop()

  }

}
