package com.github.giocode.graphxbook

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


object FlavorNetworkApp {
	def main(args: Array[String]){
		// Configure the program
		val conf = new SparkConf()
								.setAppName("Transform Flavor Network")
								.setMaster("local")
								.set("spark.driver.memory", "2G")
		val sc = new SparkContext(conf)

			
		class FNNode(val name: String)
		case class Ingredient(override val name: String, category: String) extends FNNode(name)
		case class Compound(override val name: String, cas: String) extends FNNode(name)

		val ingredients: RDD[(VertexId, FNNode)] =
			sc.textFile("./data/ingr_info.tsv").
			filter(! _.startsWith("#")).
			map {line =>
				val row = line split '\t'
				(row(0).toInt, Ingredient(row(1), row(2)))
			}
		val compounds: RDD[(VertexId, FNNode)] =
			sc.textFile("./data/comp_info.tsv").
			filter(! _.startsWith("#")).
			map {line =>
				val row = line split '\t'
				(10000L + row(0).toInt, Compound(row(1), row(2)))
			}
		val links: RDD[Edge[Int]] =
			sc.textFile("./data/ingr_comp.tsv").
			filter(! _.startsWith("#")).
			map {line =>
				val row = line split '\t'
				Edge(row(0).toInt, 10000L + row(1).toInt, 1)
			}

		// Ingredient-compound network
		val nodes = ingredients ++ compounds
		val foodNetwork = Graph(nodes, links)
		 
		// For each compound, collect ingredient Ids that has that same compound 
		// Return a collection of tuple (compound id, Array[ingredient id])
		val similarIngr: RDD[(VertexId, Array[VertexId])] = 
			foodNetwork.collectNeighborIds(EdgeDirection.In)
		 
		// This function takes a tuple of ingredient vertex id 
		def pairIngredients(ingPerComp: (VertexId, Array[VertexId])): RDD[Edge[Int]] =
			for {
				x <- ingPerComp._2
				y <- ingPerComp._2
				if x != y
			} 	yield Edge(x,y,1)

		// Create a EdgeRDD for every pair of ingredients that share same compounds
		// Duplicate edges are possible when a pair share more than one compound.
		val flavorPairsRDD: RDD[Edge[Int]] = similarIngr flatMap pairIngredients
		
		// Create a new Flavor Network connecting ingredients that share the same compound
		val flavorNetwork = Graph(ingredients, flavorPairsRDD).cache
		
		// Print 20 first triplets in flavorNetwork
		println("Previewing multi-graph flavor network:\n")
		flavorNetwork.triplets.take(20).foreach(println)
		
		// Group parallel edges between each pair into a single edge
		// The new edge contains the number of shared compounds between connected ingredients
		val flavorWeightedNetwork = flavorNetwork.partitionBy(PartitionStrategy.EdgePartition2D).
																							groupEdges((x,y) => x+y)
	  
		// Print 20 triplets with the highest number of shared compounds
	  println("\nAfter grouping parallel edges:\n")
		flavorWeightedNetwork.triplets.sortBy(t => t.attr, false).
																	 take(20).foreach(println) 
	}
}
