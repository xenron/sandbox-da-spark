package dg.spark.mllib.homework

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions.Rand
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.spark.mllib.linalg.BLAS
//import com.github.fommil.netlib.BLAS

object ch02_22 extends Serializable {

  val nDim = 5
  val nSample = 10
  val seed = 50

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("week2"))
    val predicts = predict(samples, weights)
    println(RMSE(predicts, labels))
  }

  /**
    * 大题二
    * 基于矩阵和向量进行预测
    *
    */
  val ONES = DenseVector.ones[Double](nSample)
  val MinusOnes = DenseVector.fill(nDim) { -1.0 }


  val weights = DenseVector.rand(nDim+1, Rand.gaussian)
  val samples = DenseMatrix.rand[Double](nSample, nDim)
  val labels = DenseVector.rand(nSample, Rand.uniform)

  def predict(samples: DenseMatrix[Double], w: DenseVector[Double]) = {
    require(w.size == samples.cols + 1)
    for (i <- 0 until nSample) {
      val t = samples(i, ::)//.t:DenseVector[Double]
      val rmin = min(samples(i, ::).t) //
      val rmax = max(samples(i, ::).t)
      samples(i, ::) := (samples(i, ::) - rmin) / (rmax - rmin) //归一化
    }
    val model = DenseVector.tabulate(nSample) { i =>   w(1 to -1) dot samples(i, ::).t + w(0) }
    println(model.size+model.toArray.mkString(","))

    ONES :/ (exp(model :*= -1.0) :+= 1.0)
  }


  def RMSE(predicts: DenseVector[Double], labels: DenseVector[Double]) = {
    require(predicts.size == labels.size)
    val sumSquaredDiff = sum(pow(predicts :- labels, 2))
    sqrt(sumSquaredDiff / (predicts.size - 1))
  }
}
