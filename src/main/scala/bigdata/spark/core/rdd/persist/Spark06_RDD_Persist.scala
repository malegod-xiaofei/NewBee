package bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark06_RDD_Persist {

  def main(args: Array[String]): Unit = {

    // cache : 将数据临时存储在内存中进行数据重用
    //         会在血缘关系中添加新的依赖，一旦出现问题，可以从头读取数据
    // persist：将数据临时存储在磁盘文件中进行数据重用
    //          涉及到磁盘 IO，性能较低，但是数据安全
    //          如果作业执行完毕，临时报存的数据文件就会丢失
    // checkPoint：将数据长久的保存在磁盘文件中进行数据重用
    //          涉及到磁盘 IO，性能较低，但是数据安全
    //          为了数据安全，所以一般情况下，会独立执行作业
    //          为了能够提高效率，一般情况下，是需要和 cache 联合使用
    //          执行过程中，会切断血缘关系，重新建立新的血缘关系
    //          checkpoint 等同于改变数据源

    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRdd = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRdd.map(word => {
      println("%%%%%%%")
      (word, 1)
    })

    // mapRDD.cache()
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("*******************")
    println(mapRDD.toDebugString)


    sc.stop()
  }

}
