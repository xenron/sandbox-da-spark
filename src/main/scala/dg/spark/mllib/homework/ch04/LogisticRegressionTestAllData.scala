package dg.spark.mllib.homework.ch04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegressionTestAllData {

  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示终端上
    Logger.getRootLogger.setLevel(Level.WARN)

    // 设置运行环境
    val conf = new SparkConf().setAppName("LogisticRegressionTestAllData")
    val sc = new SparkContext(conf)

    // Load and parse the data
    // 如果读入不加1，会产生两个文件，应该是默认生成了两个partition
    // 1、用户安装列表（在第1周的作业中）：
    // 用户安装列表
    // 数据格式：上报日期、用户ID、安装包名；
    // 解释下：通过第三方工具，收集到每个用户手机中的安装应用包名，收集的时间间隔是每天收集一次，所以本次数据是日期、用户、包名，也就是收集用户每一天的安装列表。
    val data_install = sc.textFile("file:///home/xenron/Documents/dataset/install")
    data_install.count
    data_install.take(1)
    // 对数据进行分割
    // org.apache.spark.rdd.RDD[(String, String)]
    val data_install_prep = data_install.map(x => {
      val line = x.split("\t")
      val date = line(0)
      val userID = line(1)
      val packageName = line(2)
      (userID, packageName)
    }).cache()
    data_install_prep.count
    data_install_prep.take(1)

    // 2、用户标签：
    // 用户安装列表
    // 数据格式：日期、用户ID、标签值（Double类型的）；
    // 解释下：通过计算，得到用户在某个标签上的分值。
    val data_label = sc.textFile("file:///home/xenron/Documents/dataset/label")
    //    val data_label = sc.textFile("file:///home/xenron/Documents/dataset/label/000005_0.gz")
    data_label.count
    data_label.take(1)
    // 对数据进行分割
    // org.apache.spark.rdd.RDD[(String, Double)]
    val data_label_prep = data_label.map(x => {
      val line = x.split("\t")
      val userID = line(0)
      val date = line(1)
      val label = line(2).toDouble
      (userID, label)
    }).cache()
    data_label_prep.count
    data_label_prep.take(1)

    // 关联数据
    // org.apache.spark.rdd.RDD[(String, (Iterable[String], Double))]
    val data_join = data_install_prep.groupByKey().join(data_label_prep)
    data_label_prep.count
    data_label_prep.take(1)

    // 特征向量
    val feature = data_install_prep.map { case (a, b) => b }.distinct.collect.zipWithIndex.toMap
    val numfeature = feature.size

    //生成labeledPoint数据
    val labeledPointData = data_join.map { r =>
      val features = Array.ofDim[Double](numfeature)
      for (_package <- r._2._1) {
        val packageIdx = feature(_package)
        features(packageIdx) = 1.0
      }
      val label = if (r._2._2.toDouble > 0.5) 1 else 0
      LabeledPoint(label, Vectors.dense(features))
    }

    labeledPointData.count
    labeledPointData.take(1)

    // 样本数据划分训练样本和测试样本
    val splits = labeledPointData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1).cache()

    // 新建逻辑回归模型，并训练
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 0.5
    val model = LogisticRegressionWithSGD.train(training, numIterations, stepSize, miniBatchFraction)
    model.weights
    model.intercept

    // 对测试样本进行测试
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }
    val print_predict = predictionAndLabels.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    // 误差计算
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)

    // 模型保存
    val ModelPath = "file:///home/xenron/Documents/output/LogisticRegressionTest"
    model.save(sc, ModelPath)

    // 读取模型，可供多次使用
    val sameModel = LogisticRegressionModel.load(sc, ModelPath)
    sameModel.weights
    sameModel.intercept
  }
}
