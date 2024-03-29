package bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2024-03-10-11:02
 */
object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统的累加器
    // Spark 默认就提供了简单数据聚合的累加器
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    // sc.doubleAccumulator
    // sc.collectionAccumulator

    rdd.foreach(
      num =>{
        // 实用累加器
        sumAcc.add(num)
      }
    )

    // 获取累加器的值
    println(sumAcc.value)

    sc.stop()
  }

}
