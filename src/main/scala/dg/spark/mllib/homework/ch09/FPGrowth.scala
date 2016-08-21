package dg.spark.mllib.homework.ch09

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据源：
  * 1、用户安装列表（在第1周的作业中）：
  * 用户安装列表，数据格式：上报日期、用户ID、安装包名；
  * 解释下：通过第三方工具，收集到每个用户手机中的安装应用包名，收集的时间间隔是每天收集一次，所以本次数据是日期、用户、包名，也就是收集用户每一天的安装列表。
  *
  * 大题1：
  * 1）选取1天的数据，取Top前1000的包名，计算包名之间的关联规则（其中阈值自己设定）。
  */
object FPGrowth {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("FPGrowth").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    val input = sc.textFile("file:/D:/user/*.gz")
    val data = input.map(_.split("\t"))

    // 对日期进行过滤
    val userAndPackage = data.filter(x =>x(0) == "2016-03-25").map(x => ( x(1),x(2)) ).cache()

    // 对包名进行累加，按照累加结果倒序排列，取得前1000位
    val top1000 = userAndPackage.map(tuple => (tuple._2,1)).reduceByKey(_+_)
      .map(tuple => (tuple._2,tuple._1)).sortByKey(false)
      .map(tuple => (tuple._2,tuple._1))
      .map(tuple => tuple._1).take(1000)

    // 建立模型
    val minSupport = 0.2
    val numPartition = 10
    val model = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartition).run(sc.parallelize(top1000))

    // 打印结果
    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
  }
}
