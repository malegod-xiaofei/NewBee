package bigdata.spark.core.framework.application

import bigdata.spark.core.framework.common.TApplication
import bigdata.spark.core.framework.controller.WordCountController

/**
 * @author Malegod_xiaofei
 * @create 2024-03-23-14:16
 */
object WordCountApplication extends App with TApplication {

  // 启动应用程序
  start() {
    val controller = new WordCountController()
    controller.dispatch()
  }

}
