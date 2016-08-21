package dg.spark.mllib.homework.ch08

import dg.spark.mllib.recommend.{ItemPref, ItemSimilarity, RecommendedItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}



object ch08_ref3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("job_09").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    val input = sc.textFile("file:/D:/user/*.gz")
    val data = input.map(_.split("\t"))
    val userAndPackage = data.filter(x =>x(0) == "2016-03-25").map(x =>
      ( x(1),x(2)) ).cache()
    val top100 = userAndPackage.map(tuple => (tuple._2,1)).reduceByKey(_+_)
      .map(tuple => (tuple._2,tuple._1)).sortByKey(false).map(tuple =>
      (tuple._2,tuple._1)).map(tuple => tuple._1).take(100)
    val itemPref = userAndPackage.filter(tuple => top100.contains(tuple._2)).map(tuple =>
      (tuple._1 + "_" + tuple._2,1))
      .reduceByKey(_+_).map{ item =>
      val splited = item._1.split("_")
      ItemPref(splited(0),splited(1),item._2)
    }
    val userdata = sc.parallelize(itemPref.take(100),8)
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(itemPref,"cooccurrence")
    val recommd = new RecommendedItem
    val recommd_rdd1 = recommd.Recommend(simil_rdd1,userdata,1)
    println(s"物品相似度矩阵:${simil_rdd1.count()}")
    simil_rdd1.collect().foreach{ ItemSimi =>
      println(ItemSimi.itemid1+"," + ItemSimi.itemid2 + "," + ItemSimi.similar)
    }
    println(s"用户推荐列表:${recommd_rdd1.count()}")
    recommd_rdd1.collect().foreach{UserRecomm =>
      println(UserRecomm.userid + "," + UserRecomm.itemid + "," + UserRecomm.pref)
    }
  }
}
