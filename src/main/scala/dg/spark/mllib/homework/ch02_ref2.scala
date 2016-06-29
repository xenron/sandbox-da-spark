package dg.spark.mllib.homework

import breeze.linalg.DenseVector
import breeze.numerics.sigmoid
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.util.{LinearDataGenerator, MLUtils}
import util.Random

object ch02_ref2 extends Serializable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Week2")
    val sc = new SparkContext(conf)

    // 10个样本，3+1(w0)，共4个纬度
    val exampleNum = 10
    val featureNum = 3

    // 随机生成RDD样本
    val data = LinearDataGenerator.generateLinearRDD(sc, exampleNum, featureNum, 10)
    // 对特征向量进行归一化, 随机生成0~1之间的label，并且增加截距项w0
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(data.map(_.features))
    val scaledData = data.map(x => (Random.nextDouble(), Vectors.dense(1 , scaler.transform(x.features).toArray : _*)))

    // 随机生成模型参数
    val model = (0 to featureNum).map( x => Random.nextDouble())
    val modelBC = sc.broadcast(model)

    // 行向量 * 列向量 => 单个元素 == 点积， 列向量 * 行向量 => 矩阵
    val breezeStep1 = scaledData.map{ case (label,vector) => (label, DenseVector(modelBC.value.toArray).t * DenseVector(vector.toArray))
      /*(label, DenseVector(vector.toArray) * DenseVector(modelBC.value.toArray).t, DenseVector(modelBC.value.toArray).t * DenseVector(vector.toArray), DenseVector(vector.toArray) dot DenseVector(modelBC.value.toArray))*/
    }

    // 生成目标结果
    val breezeStep2 = breezeStep1.map{case (label, a) => (label, sigmoid(a), a) }

    // 计算RMSE
    val rmse = scala.math.sqrt(breezeStep2.map{case (label, t, a) => scala.math.pow(t - label, 2) }.reduce(_+_) / breezeStep2.count())

    // 第二大题 Question 2
    val matrix = new RowMatrix(scaledData.map(_._2))
    val modelMatrix = Matrices.dense(featureNum + 1, 1, model.toArray)
    // step1 val fitMatrix = matrix.multiply(modelMatrix)

    // 其余步骤和大题一相同

  }
}