package dg.spark.mllib.homework.ch05

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD, NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayesTest {

  def main(args: Array[String]): Unit = {

    // 屏蔽不必要的日志显示终端上
    Logger.getRootLogger.setLevel(Level.WARN)

    // 设置运行环境
    val conf = new SparkConf().setAppName("NaiveBayesTest")
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
    val training = splits(0).cache()
    val test = splits(1).cache()

    // 新建贝叶斯分类模型，并训练
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    model.labels
    model.pi
    model.theta
    model.modelType

    // 对测试样本进行测试
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    // 统计正确率
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    // 模型保存
    val ModelPath = "file:///home/xenron/Documents/output/NaiveBayesTest"
    model.save(sc, ModelPath)

    // 读取模型，可供多次使用
    val sameModel = NaiveBayesModel.load(sc, ModelPath)
    sameModel.labels
    sameModel.pi
    sameModel.theta
    sameModel.modelType

    // PCA
//    val scaler = new StandardScaler(withMean = true, withStd = false).fit(feature)
//    val matrix = new RowMatrix(feature.map(v => scaler.transform(v)))
//    val K = 10
//    val pc = matrix.computePrincipalComponents(K)
//    val rows = pc.numRows
//    val cols = pc.numCols
//    println(rows, cols)

  }
}
