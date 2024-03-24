package bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author Malegod_xiaofei
 * @create 2024-03-24-10:49
 */
object SparkStreaming02_Queue {

  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    // StreamingContext 创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.创建 RDD 队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //4.创建 QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    //5.处理队列中的 RDD 数据
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()

    //8.循环创建并向 RDD 队列中放入 RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }

}
