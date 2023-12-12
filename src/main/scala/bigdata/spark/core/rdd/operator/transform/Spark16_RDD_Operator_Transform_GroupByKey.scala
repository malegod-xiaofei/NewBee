package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark16_RDD_Operator_Transform_GroupByKey {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - Key - Value 类型

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    // groupByKey : 将数据源中的数据，相同 key 的数据分在一个组中，形成一个对偶元组
    // 元组中的第一个元素就是 key，
    // 元组中的第二个元素就是相同 key 的 value 的集合
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupRDD.collect().foreach(println)

    val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    // groupbykey 和 reducebykey 的区别
    // groupbykey 会导致数据打乱重组，存在shuffle操作
    // reducebykey 的效率更高
    // spark 中的，shuffle 操作必须落盘处理，不能在内存中等待，会导致内存溢出，shuffle操作的性能非常低

    // reducebykey 支持分区内预聚合功能，可以有效减少shuffle时落盘的数据量 combine (预聚合)提升shuffle的性能
    // 才从功能的角度：reduceByKey其实包好分组和聚合的功能。GroupByKey只能分组，不能聚合，所以在分组聚合的场合下，推荐使用reduceByKey，如果仅仅是分组而不需要聚合。那么还是只能使用groupByKey
    sc.stop()

    // reduceByKey分区内和分区间计算规则是相同的。
  }

}
