package bigdata.spark.core.framework.controller

import bigdata.spark.core.framework.common.TController
import bigdata.spark.core.framework.service.WordCountService

/**
 * @author Malegod_xiaofei
 * @create 2024-03-23-14:16
 */
/*
 * 控制层
 */
class WordCountController extends TController {

  private val wordCountService = new WordCountService()

  // 调度
  def dispatch(): Unit = {
    // TODO 执行业务操作
    val array = wordCountService.dataAnalysis()
    array.foreach(println)
  }
}
