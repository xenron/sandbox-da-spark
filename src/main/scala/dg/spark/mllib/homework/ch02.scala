package dg.spark.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.feature.Normalizer
import breeze.linalg._
import breeze.numerics._
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.LinearDataGenerator
//import org.apache.spark.mllib.linalg.BLAS
//import com.github.fommil.netlib.BLAS

object ch02 {

  val conf=new SparkConf().setAppName("classification").setMaster("local[2]")
  val sc=new SparkContext(conf)

  def method01() = {
    // 随机生成样本（x1,...,x6）
    var x_rdd = normalVectorRDD(sc, 12L, 6, 1, 0L)
    // 随机生成标签值（y）
    var y_rdd = normalVectorRDD(sc, 12L, 1, 1, 0L)
    // 随机生成参数（w1,...,w6）
    var w_rdd = normalVectorRDD(sc, 6L, 1, 1, 0L)

    var scaler = new StandardScaler(withMean = true,withStd = true).fit(x_rdd)
//    var scalerData=data.map(point=> LabeledPoint(point.label,scaler.transform(point.features))
//    )
//    val rdd_x = LinearDataGenerator.generateLinearRDD(sc, 40000, 100, 1.0, 3, 0.5).map{
//      x=>
//        val yy=normalizer.transform(x.features)LabeledPoint(x.label,yy)
//}
//    //生成模型参数
//    val rdd_y = LinearDataGenerator.generateLinearRDD(sc, 1, 100, 1.0, 3, 0.5)
    val normalizer = new Normalizer(p = Double.PositiveInfinity)
//    var x_rdd_normalizer = normalizer.transform(x_rdd)
    var y_rdd_normalizer = normalizer.transform(y_rdd)

  }

  def method02() = {
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
