package com.github.giocode.graphxbook

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


import breeze.linalg.{SparseVector, DenseVector}
import scala.io.Source
import scala.math.abs



object joinEgoNetwork {
	def main(args: Array[String]){
		// Configure the program
		val conf = new SparkConf()
								.setAppName("Tiny Social Viz")
								.setMaster("local")
								.set("spark.driver.memory", "2G")
		val sc = new SparkContext(conf)

	type Feature = breeze.linalg.SparseVector[Int]

val featureMap: Map[Long, Feature] = 
	Source.fromFile("./data/ego.feat").
	   getLines().
	   map{line => 
		 val row = line split ' '
		 val key = abs(row.head.hashCode.toLong)
		 val feat = SparseVector(row.tail.map(_.toInt))
		 (key, feat)
	   }.toMap

	val featureNamesMap: Map[String, String] = 
		Source.fromFile("./data/ego.featnames").
		  getLines().
		  map{line => 
				val row = line.split(" ", 2)
				(row(0), row(1))
  		}.toMape


	val edges: RDD[Edge[Int]] = 
	  sc.textFile("./data/ego.edges").
	     map {line => 
	      	val row = line split ' '
	      	val srcId = abs(row(0).hashCode.toLong)
	      	val dstId = abs(row(1).hashCode.toLong)
			val srcFeat = featureMap(srcId)
			val dstFeat = featureMap(dstId)
	      	val numCommonFeats = srcFeat dot dstFeat
	      	Edge(srcId, dstId, numCommonFeats)
	     }


	val egoNetwork: Graph[Int,Int] = Graph.fromEdges(edges, 1)



}