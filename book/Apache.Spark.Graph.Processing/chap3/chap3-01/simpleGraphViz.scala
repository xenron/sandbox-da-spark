package com.github.giocode.graphxbook

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import org.graphstream.graph.{Graph => GraphStream}
import org.graphstream.graph.implementations._
import org.graphstream.ui.j2dviewer.J2DGraphRenderer

import org.jfree.chart.axis.ValueAxis
import breeze.linalg.{SparseVector, DenseVector}
import breeze.plot._
import scala.io.Source
import scala.math.abs





object SimpleGraphVizApp {
	def main(args: Array[String]){
		// Configure the program
		val conf = new SparkConf()
								.setAppName("Tiny Social Viz")
								.setMaster("local")
								.set("spark.driver.memory", "2G")
		val sc = new SparkContext(conf)

type Feature = breeze.linalg.SparseVector[Int]

// This takes the ego.feat file and creates a Map of feature
// The vertexId is created by hashing the string identifier to a Long 
val featureMap: Map[Long, Feature] = 
		Source.fromFile("../../../data/ego.feat").
						getLines().
						map{ line => 
								val row = line split ' '
								val key = abs(row.head.hashCode.toLong)
								val feat: Feature = SparseVector(row.tail.map(_.toInt))
								(key, feat)
						}.toMap


val edges: RDD[Edge[Int]] = 
	sc.textFile("../../../data/ego.edges").
		 map {line => 
				val row = line split ' '
				val srcId = abs(row(0).hashCode.toLong)
				val dstId = abs(row(1).hashCode.toLong)
				val numCommonFeats = featureMap(srcId) dot featureMap(dstId)
				Edge(srcId, dstId, numCommonFeats)
		 }

val vertices:  RDD[(VertexId, Feature)] = 
	sc.textFile("../../../data/ego.edges").
			map{line => 
					val key = abs(line.hashCode.toLong)
					(key, featureMap(key))
}

val egoNetwork: Graph[Int,Int] = Graph.fromEdges(edges, 1)
		

		// Setup GraphStream settings
		System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")

						
		// Create a SingleGraph class for GraphStream visualization
		val graph: SingleGraph = new SingleGraph("EgoSocial")

		// Set up the visual attributes for graph visualization
		graph.addAttribute("ui.stylesheet", "url(file:../../..//style/stylesheet)")
		graph.addAttribute("ui.quality")
		graph.addAttribute("ui.antialias")


		// Load the graphX vertices into GraphStream nodes
		for ((id,_) <- egoNetwork.vertices.collect()) {
		val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
		}
		// Load the graphX edges into GraphStream edges
		for (Edge(x,y,_) <- egoNetwork.edges.collect()) {
		val edge = graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
		}
		
		// Display the graph
		graph.display()


		// Function for computing degree distribution
def degreeHistogram(net: Graph[Int, Int]): Array[(Int, Int)] = 
	net.degrees.map(t => (t._2,t._1)).
		  groupByKey.map(t => (t._1,t._2.size)).
		  sortBy(_._1).collect()


val nn = egoNetwork.numVertices
val egoDegreeDistribution = degreeHistogram(egoNetwork).map({case (d,n) => (d,n.toDouble/nn)})

		// Plot degree distribution with breeze-viz
val f = Figure()
val p = f.subplot(0)
val x = new DenseVector(egoDegreeDistribution map (_._1.toDouble))
val y = new DenseVector(egoDegreeDistribution map (_._2))
p.xlabel = "Degrees"
p.ylabel = "Degree distribution"
p += plot(x, y)
// f.saveas("/output/degrees-ego.png")		

	}
}
