package bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark05_RDD_Operator_Action_SaveAsFile {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)))

    // TODO - 行动算子

    rdd.saveAsTextFile("src/main/resources/output1")
    rdd.saveAsObjectFile("src/main/resources/output2")
    rdd.saveAsSequenceFile("src/main/resources/output3")


    sc.stop()
  }

}
