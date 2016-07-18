package dg.spark.mllib.courseware.ch05

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object NaiveBayesTest {
  def main(args: Array[String]) {

    // 构建Spark对象
    val conf = new SparkConf().setAppName("naive_bayes")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据
//    val data_path = "/home/jb-huangmeiling/sample_naive_bayes_data.txt"
    val data_path = "file:///home/xenron/Documents/dataset/sample_naive_bayes_data.txt"
    val data = sc.textFile(data_path)
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    // 样本数据划分训练样本和测试样本
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

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
//    val ModelPath = "/user/huangmeiling/naive_bayes_model"
    val ModelPath = "file:///home/xenron/Documents/output/naive_bayes_model"
    model.save(sc, ModelPath)

    // 读取模型，可供多次使用
    val sameModel = NaiveBayesModel.load(sc, ModelPath)
    sameModel.labels
    sameModel.pi
    sameModel.theta
    sameModel.modelType
  }
}
