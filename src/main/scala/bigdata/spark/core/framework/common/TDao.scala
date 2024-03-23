package bigdata.spark.core.framework.common

import bigdata.spark.core.framework.util.EnvUtil

/**
 * @author Malegod_xiaofei
 * @create 2024-03-23-14:16
 */
trait TDao {

  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }
}
