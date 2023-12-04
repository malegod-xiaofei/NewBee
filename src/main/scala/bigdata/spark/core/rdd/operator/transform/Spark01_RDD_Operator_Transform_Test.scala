package bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-04-22:49
 */
object Spark01_RDD_Operator_Transform_Test {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // TODO 算子 - map
    val rdd = sc.textFile(Thread.currentThread().getContextClassLoader.getResource("datas/apache.log").getPath)

    // 长的字符串
    // 短的字符串
    val mapRDD = rdd.map(
      line => {
        val datas = line.split(" ")
        datas(6)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()

  }

}
