package dg.spark.mllib.homework.ch02

import breeze.linalg._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ch02_ref3 extends Serializable {
  def main(args: Array[String]): Unit = {
    //一、Spark ml homework w01
    //（一）Q1
    //1．Shell中初始化（spark1.6.0）
    val conf = new SparkConf().setAppName("Week2")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 3．//生成RDD样本
    val samples = 4 //生成4个样本
    val vectors = 4 //包括维度（vectors-1）维 和 label
    //生成RDD样本
    val vt = sc.parallelize(genVector(samples, vectors)).sortBy(f => f._2, true)
    vt.collect.foreach(println)
    // 4．//做归一化
    // x* = (x-min)/(max-min)
    val vt1 = vt.map(f => (((f._1.-=(min(f._1)))./=(max(f._1) - min(f._1))), f._2))
    vt1.collect.foreach(println)
    // 5．//增加1维度
    val one = DenseVector.ones[Double](1)
    val vt2 = vt1.map(f => (DenseVector.vertcat(one, f._1), f._2))
    vt2.collect.foreach(println)
    // 6．//    生成模型参数(维度+1维度)= （vectors-1 +1） =vectors
    val model = DenseVector.rand[Double](vectors)
    model.foreach(println)
    // 7．// 计算A 点乘
    val ra = vt2.map(f => DenseVector(f._1 dot model, f._2))
    ra.collect.foreach(println)
    // 8．//计算T
    val rt = ra.map(f => DenseVector(1 / (1 + 1 / math.pow(math.E, f(0))), f(1)))
    rt.collect.foreach(println)
    // 9． //    计算均方根误差
    val rmse = rt.map { x => (1, math.pow((x(0) - x(1)), 2)) }.reduceByKey((a, b) => a + b)
    val rmse2 = rmse.map(f => math.sqrt(f._2 / (samples)))
    rmse2.collect
    // （二）Q2 用矩阵重算
    // 1．//将样本中 向量和label 合并成一个向量，并且转换成一个矩阵
    val mt = vt.map(f => (1, DenseVector.vertcat(f._1, DenseVector(f._2)).toDenseMatrix)).reduceByKey((a, b) => (DenseMatrix.vertcat(a, b))).map(f => (f._2)).first()
    // 2．//做归一化  x* = (x-min)/(max-min)
    var f = 0
    for (f <- 0 to samples - 1) {
      var vMax = max(mt(f, 0 to vectors - 2))
      var vMin = min(mt(f, 0 to vectors - 2))
      mt(f, 0 to vectors - 2).-=(vMin)./=(vMax - vMin)
    }
    // 3． //增加1维度
    val mOnes = DenseMatrix.ones[Double](samples, 1)
    val mtO = DenseMatrix.horzcat(mOnes, mt)
    // 4． //根据现有矩阵的标签排序后重构矩阵
    // 因为第一步从RDD样本转换成矩阵后的行顺序变化了，所以样本会和第一题生成的模型的顺序不匹配，会造成计算A的结果和第一题不一致，因此需要按照RDD样本数据的顺序重构矩阵
    // （1）//拆成向量组成的list
    var mtOs: List[(DenseVector[Double], Double)] = List()
    f = 0
    for (f <- 0 to samples - 1) {
      mtOs = mtOs.::(mtO(f, 0 to vectors).inner, mtO(f, -1))
    }
    // （2） //排序后重构 bef
    f = 0
    val it = mtOs.sortBy(f => f._2).iterator
    var arry: (DenseVector[Double], Double) = it.next()
    var bef = arry._1.toDenseMatrix
    var aft = arry._1.toDenseMatrix
    for (f <- 1 to samples - 1) {
      arry = it.next()
      aft = arry._1.toDenseMatrix
      bef = DenseMatrix.vertcat(bef, aft)
    }
    // 5．//    根据第一题生成的模型转换成矩阵
    val mModel = DenseMatrix(model.toArray)
    // 6． // 计算A:mra 点乘
    val mra = DenseMatrix.horzcat(bef(0 to samples - 1, 0 to vectors - 1).*(mModel.t), bef(::, vectors).toDenseMatrix.t)
    // 7．//计算T:mrt
    // （1）//    方法一
    f = 0
    var mrary: Array[Double] = Array()
    for (f <- 0 to samples - 1) {
      mrary = mrary.:+(1 / (1 + 1 / math.pow(math.E, mra(f, 0))))
    }
    val mrt = DenseMatrix.horzcat(DenseMatrix(mrary).t, mra(::, 1).toDenseMatrix.t)
    // （2）//    方法二 mrt2
    val mrt2 = DenseMatrix.horzcat(mra(::, 0).
      map { x => 1 / (1 + 1 / math.pow(math.E, x)) }.toDenseMatrix.t,
      mra(::, 1).toDenseMatrix.t)
    // 8．//    计算均方根误差
    val mrmse = math.sqrt(sum((mrt(::, 0) - mrt(::, 1)).map { x => (math.pow(x, 2)) }) / samples)
    // （1）和第一题的均方根误差结果相同
    // 第一题均方根误差结果：

  }

  //2．生成样本函数
  /**
    * *
    * samples:样本数量
    * vectors:包括（ 维度数量+label维度）
    */
  def genVector(samples: Int, vectors: Int): List[(DenseVector[Double], Double)] = {
    var list: List[(DenseVector[Double], Double)] = List()
    var x = 0
    for (x <- 0 to samples - 1) {
      var rnd = DenseVector.rand(vectors)
      list = list.::(rnd.copy(0 to (vectors - 2)).*=(100.0), rnd(-1))
      //      println(DenseVector.rand(5))
      //      println(list.size)
    }
    list
  }
}