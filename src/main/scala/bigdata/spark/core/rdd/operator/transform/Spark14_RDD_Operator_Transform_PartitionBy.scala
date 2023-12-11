package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark14_RDD_Operator_Transform_PartitionBy {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - Key - Value 类型

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    // RDD => PairRDDFunctions
    // 隐式转换（二次编译）

    // partitionBy 根据指定的分区规则对数据进行重分区
    // 数据对分区取模
    mapRDD.partitionBy(new HashPartitioner(2))
      .saveAsTextFile("src/main/resources/output2")

    sc.stop()

  }

}
