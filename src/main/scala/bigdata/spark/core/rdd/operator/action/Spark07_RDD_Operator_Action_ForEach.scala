package bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2023-12-21-22:28
 */
object Spark07_RDD_Operator_Action_ForEach {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkconf)

    // 闭包检查
    // val rdd = sc.makeRDD(List[Int]())
    val rdd = sc.makeRDD(List[Int](1, 2, 3, 4))

    val user = new User()
    // SparkException: Task not serializable
    // 从计算的角度，算子以外的代码在 Driver 端执行，算子里面的代码都是在 Executor 端执行

    // RDD 算子中传递的函数是会包含闭包操作，那么就会进行检测功能
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )

    sc.stop()
  }

  // class User extends Serializable {
  // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
  // case class User() {
  class User {
    var age: Int = 30
  }

}
