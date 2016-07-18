package dg.spark.mllib.courseware.ch04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.{LabeledPoint}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegression {

  def main(args: Array[String]) {
    // 构建Spark对象
    val conf = new SparkConf().setAppName("LogisticRegression")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据1，格式为LIBSCM format
//    val data = MLUtils.loadLibSVMFile(sc, "hdfs://192.168.180.79:9000/user/huangmeiling/sample_libsvm_data.txt")
    val data_path = "file:///home/xenron/Documents/dataset/sample_libsvm_data.txt"
    val data = MLUtils.loadLibSVMFile(sc, data_path)
    data.count
    data.take(1)

    // 样本数据划分训练样本和测试样本
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // 新建逻辑回归模型，并训练
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 0.5
    val model = LogisticRegressionWithSGD.train(training, numIterations, stepSize, miniBatchFraction)
    //    val model = new LogisticRegressionWithLBFGS().
    //      setNumClasses(10).
    //      run(training)
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
//    val ModelPath = "hdfs://192.168.180.79:9000/user/huangmeiling/logistic_regression_model"
    val ModelPath = "file:///home/xenron/Documents/output/logistic_regression_model"
    model.save(sc, ModelPath)

    // 读取模型，可供多次使用
    val sameModel = LogisticRegressionModel.load(sc, ModelPath)
    sameModel.weights
    sameModel.intercept

  }

}