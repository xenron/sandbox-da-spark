package dg.spark.mllib.homework.ch09

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

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
