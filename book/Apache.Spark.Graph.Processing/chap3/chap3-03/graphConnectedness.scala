package com.github.giocode.graphxbook

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// import org.graphstream.graph.{Graph => GraphStream}
// import org.graphstream.graph.implementations._
// import org.graphstream.ui.j2dviewer.J2DGraphRenderer

// import org.jfree.chart.axis.ValueAxis
// import breeze.linalg._
// import breeze.plot._
// import scala.io.Source
// import scala.math.abs


object WebPageRankApp {
    def main(args: Array[String]){
        // Configure the program
        val conf = new SparkConf()
                    .setAppName("NotreDame Webpage Ranking")
                    .setMaster("local")
                    .set("spark.driver.memory", "2G")
        val sc = new SparkContext(conf)

        // Load web graph
        val webGraph = GraphLoader.edgeListFile(sc,"./data/web-NotreDame.txt")
    
    	// Run PageRank with a tolerance value of 0.0001
       	val ranks = webGraph.pageRank(0.001).vertices

    }
}
