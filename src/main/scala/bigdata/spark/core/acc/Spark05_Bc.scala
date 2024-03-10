package bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Malegod_xiaofei
 * @create 2024-03-10-11:02
 */
object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    //    val rdd2 = sc.makeRDD(List(
    //      ("a", 4), ("b", 5), ("c", 6)
    //    ))

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // join 会导致数据量几何增长，并且会影响 shuffle 的性能，不推荐使用
    // val joinRdd: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    // joinRdd.collect().foreach(println)

    // (a,1),    (b,2),    (c,3)
    // (a,(1,4)),(b,(2,5)),(c,(3,6))

    rdd1.map {
      case (w, c) => {
        val l: Int = map.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)

    sc.stop()
  }

}
