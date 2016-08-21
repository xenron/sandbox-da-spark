package dg.spark.mllib.homework.ch08

import dg.spark.mllib.recommend.{ItemPref, ItemSimilarity, RecommendedItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据源：
  * 1、用户安装列表（在第1周的作业中）：
  * 用户安装列表，数据格式：上报日期、用户ID、安装包名；
  * 解释下：通过第三方工具，收集到每个用户手机中的安装应用包名，收集的时间间隔是每天收集一次，
  * 所以本次数据是日期、用户、包名，也就是收集用户每一天的安装列表。
  *
  * 数据源：
  * 1、用户安装列表（在第1周的作业中）：
  * 用户安装列表，数据格式：上报日期、用户ID、安装包名；
  * 解释下：通过第三方工具，收集到每个用户手机中的安装应用包名，收集的时间间隔是每天收集一次，所以本次数据是日期、用户、包名，也就是收集用户每一天的安装列表。
  * 2、用户标签：
  * 用户安装列表，数据格式：日期、用户ID、标签值（Double类型的）；
  * 解释下：通过计算，得到用户在某个标签上的分值。
  *
  * 大题1：
  * 1）选取1天的数据，取Top前100的包名，计算包名之间的相似度。
  * 2）根据相似度矩阵，随机抽取100用户，计算用户的推荐结果。
  *
  */
object ch08_ref1 {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR);

    val conf = new SparkConf().setAppName("recommend").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val userlist = sc.textFile("/mllib/userlist")
    userlist.take(10).foreach(println)

    val usertag = sc.textFile("/mllib/usertag")
    usertag.take(10).foreach(println)

    val userdata = userlist.map { x =>
      val fields = x.split("\t")
      (fields(1), fields(0), fields(2))
    }.filter(_._2.equals("2016-03-31")).map(x => (x._1, x._3))


    val tagdata = usertag.map { x =>
      val fields = x.split("\t")
      (fields(0), fields(1), fields(2))
    }.filter(_._2.equals("2016-03-31")).map(x => (x._1, x._3))


    val itemData = userdata.join(tagdata).map(x => {
      (ItemPref(x._1, x._2._1, x._2._2.toDouble))
    })


    //2 建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(itemData, "cooccurrence")
    val recommd = new RecommendedItem
    val recommd_rdd1 = recommd.Recommend(simil_rdd1, itemData, 30)


    //3 打印结果
    println(s"物品相似度矩阵: ")
    simil_rdd1.take(100).foreach { ItemSimi =>
      println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
    }
    println(s"用戶推荐列表: ")
    recommd_rdd1.take(100).foreach { UserRecomm =>
      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " + UserRecomm.pref)
    }

  }
}