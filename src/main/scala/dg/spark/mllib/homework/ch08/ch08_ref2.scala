package dg.spark.mllib.homework.ch08

import dg.spark.mllib.recommend.{ItemPref, ItemSimilarity, RecommendedItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 基于物品的协同过滤
  */
object ch08_ref2 {
  def main(args: Array[String]) {
    // 构建 Spark 对象
    val conf = new SparkConf().setAppName("logistic_regression")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)
    // 读取用户安装列表数据
    val rawData = sc.textFile("UserData1").map(_.split("\t"))
    // 选取 2016-03-29 一天的数据
    val userData = rawData.filter(x => x(0) == "2016-03-29").map(x =>
      (x(2),x(1)))
    // 计算每个安装包的用户数
    val itemData = userData.map(x => (x._1, 1))
    // 取用户数最多的前 100 个安装包
    val itemCount = itemData.reduceByKey((x, y) => x + y).sortBy(x =>
      x._2, false).take(100)
    val itemCount1 = sc.parallelize(itemCount)
    val userData1 = userData.join(itemCount1)
    // 用户名，安装包，得分
    val userData2 = userData1.map(x => ItemPref(x._2._1, x._1,
      1.0)).cache()
    // 建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(userData2, "cooccurrence")
    val recommd = new RecommendedItem
    val recommd_rdd1 = recommd.Recommend(simil_rdd1, userData2, 1)
    // 输出前 100 个用户的推荐结果
    println(s"用戶推荐列表")
    recommd_rdd1.take(100).foreach { UserRecomm =>
      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " +
        UserRecomm.pref)
    }
  }

}
