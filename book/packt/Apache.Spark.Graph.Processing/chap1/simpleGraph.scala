package com.github.giocode.graphxbook

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object SimpleGraphApp {
	def main(args: Array[String]){
		// Configure the program
		val conf = new SparkConf()
					.setAppName("Tiny Social")
					.setMaster("local")
					.set("spark.driver.memory", "2G")
		val sc = new SparkContext(conf)

		// Load some data into RDDs
		val people = sc.textFile("../../data/people.csv")
		val links = sc.textFile("../../data/links.csv")

		// Parse the csv files into new RDDs
		case class Person(name: String, age: Int)
		type Connexion = String
		val peopleRDD: RDD[(VertexId, Person)] = people map { line => 
			val row = line split ','
			(row(0).toInt, Person(row(1), row(2).toInt))
		}
		val linksRDD: RDD[Edge[Connexion]] = links map {line => 
			val row = line split ','
			Edge(row(0).toInt, row(1).toInt, row(2))
		}

		// Create the social graph of people
		val tinySocial: Graph[Person, Connexion] = Graph(peopleRDD, linksRDD)
		tinySocial.cache()
		tinySocial.vertices.collect()
		tinySocial.edges.collect()

		// Extract links between coworkers and print their professional relationship
		val profLinks: List[Connexion] = List("coworker", "boss", "employee","client", "supplier")
		tinySocial.subgraph(profLinks contains _.attr).
				   triplets.foreach(t => println(t.srcAttr.name + " is a " + t.attr + " of " + t.dstAttr.name))
	}
}
