package dg.spark.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.feature.Normalizer
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{LinearDataGenerator, MLUtils}

//import org.apache.spark.mllib.linalg.BLAS
//import com.github.fommil.netlib.BLAS

object ch02_11 {

  val conf = new SparkConf().setAppName("classification").setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    //读取数据
    val data = MLUtils.loadLibSVMFile(sc, "/tmp/sample_libsvm_data.txt").map(x => LabeledPoint(x.label, x.features.toDense))
    val vectors = data.map(lp => lp.features)
    val data_num = data.count()
    //归一化：使用 SparkStandardScaler 中的方法完成 x-u/sqrt(variance)
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    val scaledData = data.map(lp => LabeledPoint(lp.label, scaler.transform(lp.features)))
    //增加截断
    val dataAndBias = scaledData.map(lp => (lp.label, appendBias(lp.features)))
    //随机初始化权重 w
    val initialWeights = DenseVector.rand(vectors.first().size + 1)
    //计算
    val predictionAndLabel = dataAndBias.map(x => {
      val a = breeze.linalg.DenseVector(x._2.toArray) dot
        breeze.linalg.DenseVector(initialWeights.toArray)
      val t = 1.0 / (1.0 + exp(-1.0 * a))
      (t, x._1)
    })
    //RMSE
    val loss = predictionAndLabel.map { x =>
      val err = x._1 - x._2
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / data_num)
    println(s"RMSE = $rmse")
  }

  def appendBias(inputVector: Vector): Vector = {
    val outputVector = DenseVector.ones[Double](inputVector.size + 1)
    outputVector(1 to -1) := DenseVector(inputVector.toArray)
    Vectors.dense(outputVector.toArray)
  }
}
