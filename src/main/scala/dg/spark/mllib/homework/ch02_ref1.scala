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

object ch02_ref1 {
}
