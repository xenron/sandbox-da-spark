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
import breeze.linalg._
import breeze.plot._
import scala.io.Source
import scala.math.abs


object GraphConnectednessApp {
    def main(args: Array[String]){
        // Configure the program
        val conf = new SparkConf()
                    .setAppName("Food Network Components")
                    .setMaster("local")
                    .set("spark.driver.memory", "2G")
        val sc = new SparkContext(conf)

        // Bipartite network nodes' class hierarchy
        class FNNode(val name: String)
        case class Ingredient(override val name: String, category: String) extends FNNode(name)
        case class Compound(override val name: String, cas: String) extends FNNode(name)
        
        // Load ingredient nodes
        val ingredients: RDD[(VertexId, FNNode)] =
          sc.textFile("./data/ingr_info.tsv").
          filter(! _.startsWith("#")).
          map {line =>
          val row = line split '\t'
          (row(0).toInt, Ingredient(row(1), row(2)))
        }
        
        // Load compound nodes
        val compounds: RDD[(VertexId, FNNode)] =
          sc.textFile("./data/comp_info.tsv").
          filter(! _.startsWith("#")).
          map {line =>
          val row = line split '\t'
          (10000L + row(0).toInt, Compound(row(1), row(2)))
        }

        // Load links
        val links: RDD[Edge[Int]] =
          sc.textFile("./data/ingr_comp.tsv").
          filter(! _.startsWith("#")).
          map {line =>
          val row = line split '\t'
          Edge(row(0).toInt, 10000L + row(1).toInt, 1)
        }

        // Create food network
        val nodes = ingredients ++ compounds         
        val foodNetwork = Graph(nodes, links)

        // Setup GraphStream settings
        System.setProperty("org.graphstream.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")

        // Create a SingleGraph class for GraphStream visualization
        val graph: SingleGraph = new SingleGraph("FoodNetwork")
         
        // Set up the visual attributes for graph visualization
        graph.addAttribute("ui.stylesheet", "url(file:.//style/stylesheet-foodnetwork)")
        graph.addAttribute("ui.quality")
        graph.addAttribute("ui.antialias")
         
        // Load the graphX vertices into GraphStream nodes
        for ((id: VertexId, fnn: FNNode) <- foodNetwork.vertices.collect()) {
          val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
          node.addAttribute("name", fnn.name)
          node.addAttribute("ui.label", fnn.name)
        }
        // Load the graphX edges into GraphStream edges
        for (Edge(x,y,_) <- foodNetwork.edges.collect()) {
          val edge = graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
        }
        // Display the graph
        graph.display()

    }
}
