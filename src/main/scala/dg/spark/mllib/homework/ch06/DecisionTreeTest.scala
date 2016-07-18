package dg.spark.mllib.homework.ch06

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeTest {

  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示终端上
    Logger.getRootLogger.setLevel(Level.WARN)

    // 设置运行环境
    val conf = new SparkConf().setAppName("DecisionTreeTest")
    val sc = new SparkContext(conf)

    // Load and parse the data
    // 如果读入不加1，会产生两个文件，应该是默认生成了两个partition
    // 1、用户安装列表（在第1周的作业中）：
    // 用户安装列表
    // 数据格式：上报日期、用户ID、安装包名；
    // 解释下：通过第三方工具，收集到每个用户手机中的安装应用包名，收集的时间间隔是每天收集一次，所以本次数据是日期、用户、包名，也就是收集用户每一天的安装列表。
    val data_install = sc.textFile("file:///home/xenron/Documents/dataset/install/000005_0.gz")
    data_install.count
    data_install.take(1)
    // 对数据进行分割，筛选符合日期的数据（2016-03-29）
    // Array[(String, (String, String))]
    val data_install_prep = data_install.map(x => {
      val line = x.split("\t")
      val date = line(0)
      val userID = line(1)
      val packageName = line(2)
      (userID, (packageName, date))
    }).filter(_._2._2 == "2016-03-29").cache()
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
    // 对数据进行分割，筛选符合日期的数据（2016-03-29）
    // Array[(String, (Double, String))]
    val data_label_prep = data_label.map(x => {
      val line = x.split("\t")
      val userID = line(0)
      val date = line(1)
      val label = line(2).toDouble
      (userID, (label, date))
    }).filter(_._2._2 == "2016-03-29").cache()
    data_label_prep.count
    data_label_prep.take(1)

    // 关联数据
    // Array[(String, (Iterable[(String, String)], (Double, String)))]
    val data_join = data_install_prep.groupByKey().join(data_label_prep)
    data_label_prep.count
    data_label_prep.take(1)

    // 特征向量
    val feature = data_install_prep.map { case (a, (b, c)) => b }.distinct.collect.zipWithIndex.toMap
    val numfeature = feature.size

    //生成labeledPoint数据
    val labeledPointData = data_join.map { r =>
      val features = Array.ofDim[Double](numfeature)
      for (_package <- r._2._1) {
        val packageIdx = feature(_package._1)
        features(packageIdx) = 1.0
      }
      val label = if (r._2._2._1.toDouble < 0.5) 0 else if (r._2._2._1.toDouble > 1) 2 else 1
      LabeledPoint(label, Vectors.dense(features))
    }

    labeledPointData.count
    labeledPointData.take(1)

    // 样本数据划分训练样本和测试样本
    val splits = labeledPointData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0).cache()
    val testData = splits(1).cache()

    // 新建决策树
    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // 误差计算
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val print_predict = labelAndPreds.take(20)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    // 保存模型
    //    val ModelPath = "/user/huangmeiling/Decision_Tree_Model"
    val ModelPath = "file:///home/xenron/Documents/output/Decision_Tree_Model"
    model.save(sc, ModelPath)
    val sameModel = DecisionTreeModel.load(sc, ModelPath)

  }
}
