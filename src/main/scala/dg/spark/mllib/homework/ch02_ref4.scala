package dg.spark.mllib.homework

import breeze.numerics._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.{MLUtils, LinearDataGenerator}
import org.apache.spark.{SparkContext, SparkConf}
import breeze.linalg.{*, DenseVector, DenseMatrix}
import breeze.stats._
import org.apache.spark.{SparkContext, SparkConf}

object ch02_ref4 extends Serializable {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    test01(sc)
    test02(sc)
  }

  def appendBias(inputVector: Vector): Vector = {
    val outputVector = DenseVector.ones[Double](inputVector.size + 1)
    outputVector(1 to -1) := DenseVector(inputVector.toArray)
    Vectors.dense(outputVector.toArray)
  }

  def test01(sc: SparkContext) = {
    //读取数据
    val data = MLUtils.loadLibSVMFile(sc, "E:\\softwares\\spark-1.6.0-binhadoop2.6\\data\\mllib/sample_libsvm_data.txt").map(x =>
      LabeledPoint(x.label, x.features.toDense))
    val vectors = data.map(lp => lp.features)
    val data_num = data.count()
    //归一化：使用SparkStandardScaler 中的方法完成 x-u/sqrt(variance)
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    val scaledData = data.map(lp => LabeledPoint(lp.label, scaler.transform(lp.features)))
    //增加截断
    val dataAndBias = scaledData.map(lp => (lp.label, appendBias(lp.features)))
    //随机初始化权重w
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

  def test02(sc: SparkContext) = {
    var nums = 1000;
    var features = 50
    //生成数据
    val featuresMatrix = DenseMatrix.rand[Double](nums,features)
    val labelMatrix = DenseMatrix.rand[Double](nums,1)
    //求均值和方差
    val featuresMean = mean(featuresMatrix(::, *)).toDenseVector
    val featuresStddev = stddev(featuresMatrix(::,*)).toDenseVector
    //归一化
    featuresMatrix(*,::) -= featuresMean
    featuresMatrix(*,::) /= featuresStddev
    //增加截距
    val intercept = DenseMatrix.ones[Double](featuresMatrix.rows,1)
    val train = DenseMatrix.horzcat(intercept,featuresMatrix)
    //计算
    val w = DenseMatrix.rand[Double](features+1,1)
    val A = (train * w).asInstanceOf[DenseMatrix[Double]]
    val probability = 1.0/(exp(A * -1.0) + 1.0)
    //RMSE
    val RMSE = sqrt(mean(pow(probability - labelMatrix,2)))
    println(RMSE)
  }
}
