package bigdata.spark.core.framework.common

import bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2024-03-23-12:49
 */
trait TApplication {
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {
    val sparConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparConf)

    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    // TODO 关闭链接
    sc.stop()
    EnvUtil.clear()

  }

}
