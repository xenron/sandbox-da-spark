package dg.spark.mllib.homework.ch07

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object KMeansTestForRandam {

  def main(args: Array[String]) {
    // 构建Spark对象
    val conf = new SparkConf().setAppName("KMeans")

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 创建随机的二维数组
    val rows = 100
    val cols = 2
    val rand = new Random()
    val data = Array.ofDim[Double](rows, cols)
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        // 将10组数据进行分隔
        data(i)(j) = i / 10 + rand.nextDouble()
      }
    }

    val dataTrans = data.map(x => Vectors.dense(x))
    val parsedData = sc.parallelize(dataTrans, 1)

    // 设置簇的个数为10
    val numClusters = 10
    // 迭代20次
    val numIterations = 20
    // 运行1000次,选出最优解
    val runs = 1000
    // 设置初始K选取方式为k-means++
    val initMode = "k-means||"
    val clusters = new KMeans().setInitializationMode(initMode).setK(numClusters).setMaxIterations(numIterations).run(parsedData)

    // 打印出测试数据属于哪个簇
    println(parsedData.map(v => v.toString() + " belong to cluster :" + clusters.predict(v)).collect().mkString("\n"))

    // Evaluateclustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("WithinSet Sum of Squared Errors = " + WSSSE)

    val point1 = clusters.predict(Vectors.dense(1.2, 1.3))
    val point2 = clusters.predict(Vectors.dense(4.1, 4.2))

    // 打印出中心点
    println("Clustercenters:")
    for (center <- clusters.clusterCenters) {
      println(" " + center)
    }

    println("Prediction of (1.2,1.3)--> " + point1)
    println("Prediction of (4.1,4.2)--> " + point2)

    //保存模型
    //    val ModelPath = "/user/xenron/Documents/output/KMeans_Model"
    //    model.save(sc, ModelPath)
    //    val sameModel = KMeansModel.load(sc, ModelPath)

  }

}

