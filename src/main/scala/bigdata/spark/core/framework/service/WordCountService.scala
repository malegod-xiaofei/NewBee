package bigdata.spark.core.framework.service

import bigdata.spark.core.framework.common.TService
import bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author Malegod_xiaofei
 * @create 2024-03-23-14:16
 *
 */
/*
 * 服务层
 */
class WordCountService extends TService {

  private val wordCountDao = new WordCountDao()

  // 数据分析
  def dataAnalysis() = {

    val lines = wordCountDao.readFile("src/main/resources/datas/word.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne = words.map(word => (word, 1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val array: Array[(String, Int)] = wordToSum.collect()
    array
  }
}
