package dg.spark.mllib.courseware.ch03

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel

object LinearRegression {

  def main(args: Array[String]) {

    // 构建Spark对象
    val conf = new SparkConf().setAppName("LinearRegressionWithSGD")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据
//    val data_path = "/home/jb-huangmeiling/lpsa.data"
//    val data_path = "hdfs:///~/Document/dataset/lpsa.data"
    val data_path = "file:///home/xenron/Documents/dataset/lpsa.data"
    val data = sc.textFile(data_path)
    val examples = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    // 归一化处理

    examples.take(1)
    val numExamples = examples.count()

    // 新建线性回归模型，并设置训练参数
    val numIterations = 100
    val stepSize = 1
    val miniBatchFraction = 1.0
    val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
    model.weights
    model.intercept

    // 对样本进行测试
    val prediction = model.predict(examples.map(_.features))
    val predictionAndLabel = prediction.zip(examples.map(_.label))

    // 取得前20条数据，打印出来，进行比较
    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    // 计算测试误差
    val loss = predictionAndLabel.map {
      case (p, l) =>
        val err = p - l
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numExamples)
    println(s"Test RMSE = $rmse.")

    // 模型保存
//    val ModelPath = "hdfs://192.168.180.79:9000/user/xenron/logistic_regression_model"
    val ModelPath = "file:///home/xenron/Documents/output/LinearRegressionModel"
    model.save(sc, ModelPath)

    // 读取模型，可供多次使用
    val sameModel = LinearRegressionModel.load(sc, ModelPath)
    sameModel.weights
    sameModel.intercept

  }

}

