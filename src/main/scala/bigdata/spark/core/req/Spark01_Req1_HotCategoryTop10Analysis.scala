package bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Malegod_xiaofei
 * @create 2024-03-11-16:54
 */
object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {

    // TODO：6.1Top10 热门品类
    val sparConf = new SparkConf().setMaster("local").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparConf)

    // 1.读取原始日志数据
    val actionRdd = sc.textFile("src/main/resources/datas/user_visit_action.txt")

    // 2.统计品类的点击数量：（品类ID，点击数量）
    val clickActionRdd: RDD[String] = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    val clickCountRdd: RDD[(String, Int)] = clickActionRdd.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3.统计品类的下单数量：（品类ID，下单数量）
    val orderActionRdd: RDD[String] = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(8) != null
      }
    )

    // orderid : 1,2,3
    // [(1,1),(2,1),(3,1)]
    val orderCountRdd: RDD[(String, Int)] = orderActionRdd.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4.统计品类的支付数量：（品类ID，支付数量）
    val payActionRdd: RDD[String] = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(10) != null
      }
    )

    // orderid : 1,2,3
    // [(1,1),(2,1),(3,1)]
    val payCountRdd: RDD[(String, Int)] = payActionRdd.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 5.将品类进行排序，并且取前 10 名
    //   点击数量排序，下单数量排序，支付数量排序
    //   元组排序（多个无关数据所组成的集合）：先比较第一个，再比较第二个，再比较第三个，依次类推
    //   （品类ID，（点击数量，下单数量，支付数量））
    // cogroup
    val coGroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
    clickCountRdd.cogroup(orderCountRdd, payCountRdd)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = coGroupRDD.mapValues {
      case (clicksIter, ordersIter, paysIter) => {
        var clickCnt = 0
        val iter1: Iterator[Int] = clicksIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2: Iterator[Int] = ordersIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3: Iterator[Int] = paysIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    // 6.将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }

}