package dg.spark.mllib.homework.ch07


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.Vectors

object KMeansTestForInput {

  def main(args: Array[String]) {
    //1 构建Spark对象
    //    val conf = new SparkConf().setAppName("KMeans")
    val conf = new SparkConf().setAppName("KMeans").setMaster("spark://xenron-XPS-8700:7077") //.set("spark.ui.port","8080")

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据1，格式为LIBSVM format
    val data = sc.textFile("hdfs://xenron-XPS-8700:9000/input/kmeans_data.txt")
    //    val data = sc.textFile("/home/xenron/Documents/dataset/sample_kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.trim.toDouble))).cache()

    //设置簇的个数为3
    val numClusters = 3
    //迭代20次
    val numIterations = 20
    //运行10次,选出最优解
    val runs = 10
    //设置初始K选取方式为k-means++
    val initMode = "k-means||"
    val clusters = new KMeans().
      setInitializationMode(initMode).
      setK(numClusters).
      setMaxIterations(numIterations).
      run(parsedData)

    //打印出测试数据属于哪个簇
    println(parsedData.map(v => v.toString() + " belong to cluster :" + clusters.predict(v)).collect().mkString("\n"))

    // Evaluateclustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("WithinSet Sum of Squared Errors = " + WSSSE)

    val a21 = clusters.predict(Vectors.dense(1.2, 1.3, 1.4))
    val a22 = clusters.predict(Vectors.dense(4.1, 4.2, 4.3))

    //打印出中心点
    println("Clustercenters:")
    for (center <- clusters.clusterCenters) {
      println(" " + center)
    }

    println("Prediction of (1.2,1.3)--> " + a21)
    println("Prediction of (4.1,4.2)--> " + a22)

    //保存模型
    //    val ModelPath = "/user/xenron/Documents/output/KMeans_Model"
    //    model.save(sc, ModelPath)
    //    val sameModel = KMeansModel.load(sc, ModelPath)

  }

}

