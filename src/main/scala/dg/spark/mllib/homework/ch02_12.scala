package dg.spark.mllib.homework

import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.feature.{Normalizer, StandardScaler}
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.mllib.linalg.BLAS
//import com.github.fommil.netlib.BLAS

/**
  * 基于矩阵和向量进行预测
  */
object ch02_12 {

  val conf=new SparkConf().setAppName("classification").setMaster("local[2]")
  val sc=new SparkContext(conf)

  def main(args: Array[String]) {
    // 随机生成样本（x1,...,x6）
    var x = DenseMatrix.rand(12, 6)
    // 随机生成标签值（y）
    var y = DenseVector.rand(12)
    // 随机生成参数（w1,...,w6）
    var w = DenseVector.rand(6)
    var A = x * w
    var T = 1.0 / (pow(exp(1.0),-A) + 1.0)
    var RMSE = pow(sum(pow(y-T,2)) / y.length, 0.5)
  }
}
