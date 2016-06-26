package dg.spark.mllib.homework

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ch02_21 extends Serializable {

  val nDim = 5
  val nSample = 10
  val seed = 50
  /**
    * 基于RDD的随机生成模型参数
    */
  val w0 = math.random

  def genLabels(nSample: Int) = for(i <- 0 until nSample) yield math.random

  def genWeights(nDim: Int) = for(i <- 0 until nDim) yield math.random

  def genOneDimSample(nSample: Int) = for(i <- 0 until nSample) yield math.random*seed

  def genOneRowSample(nDim: Int) = for(i <- 0 until nDim) yield math.random*seed

  def genModelSamples(nSample: Int, nDim: Int, @transient sc: SparkContext) =
    for(i <- 0 until nSample) yield genOneRowSample(nDim)//genOneDimSample(nSample)//sc.parallelize()

  def genRDDWeights(nDim: Int, @transient sc: SparkContext) = sc.parallelize(genWeights(nDim))

  def genRDDSamples(nSample: Int, nDim: Int, @transient sc: SparkContext): RDD[Seq[Double]] =
    sc.parallelize(genModelSamples(nSample, nDim, sc))

  /**
    * 对样本进行行归一化,并进行逻辑回归预测
    */
  def normalizeSamples(samples: RDD[Seq[Double]]): RDD[Seq[Double]] = {
    samples.map { row =>
      val min = row.min
      val max = row.max
      val maxInterval = max - min
      val normalizedRow = row.map { x => (x-min)/maxInterval }
      normalizedRow
    }
  }

  private def RDDproduct(v1: RDD[Double], v2: RDD[Double]) = {
    require(v2.count() == v1.count )
    w0 + v2.zip(v1).treeAggregate(0.0)(seqOp = (c, v) => c+v._1*v._2, combOp = (c1, c2) => c1 + c2)
  }

  private def product(v1: Seq[Double], v2: Seq[Double]) = {
    require(v2.size == v1.size )
    w0 + v2.zip(v1).map(pair => pair._1*pair._2).sum
  }

  def trainModel(@transient sc: SparkContext): RDD[Double]= {//, samples: RDD[Seq[Double]], weights: RDD[Double]
  val samples = genRDDSamples(nSample, nDim, sc)
    val weights = genWeights(nDim)
    val input = samples.map { x =>
      product(x,weights) }//normalizeSamples()
    input.map { x =>
      1/(1 + math.exp(-x))
    }
  }

  /**
    * 最后计算所有样本的RMSE
    * RMSE=sqrt(sum((predict - label)^2)/ (n-1))
    */
  def RMSE(predicts: RDD[Double], @transient sc: SparkContext ) = {
    val n = predicts.count().intValue()
    val rddLabels = sc.parallelize(genLabels(n))
    val sumResidue = predicts.zip(rddLabels).treeAggregate(0.0)(seqOp = (c, v) => c+(v._1-v._2)*(v._1-v._2), combOp = (c1, c2) => c1 + c2)
    math.sqrt(sumResidue/(n-1))
  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("week2"))
    val RDDpredicts = trainModel(sc)
    val rmse = RMSE(RDDpredicts, sc)
    println(rmse)
  }
}
