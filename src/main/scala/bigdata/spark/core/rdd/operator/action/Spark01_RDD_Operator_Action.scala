package bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark01_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子
    // 所谓的行动算子，其实就是触发作业（job）执行的方法
    // 底层代码调用的是环境对象的 runjob() 方法
    // 底层代码中创建 ActiveJob，并提交执行。

    rdd.collect()

    sc.stop()
  }

}
