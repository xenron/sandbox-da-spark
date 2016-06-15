package com.github.giocode.graphxbook

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.io.Source
import org.apache.spark.mllib.clustering.{PowerIterationClustering, PowerIterationClusteringModel}
import org.apache.spark.mllib.linalg.Vectors


object MusicPlaylistApp {
    def main(args: Array[String]){
        // Configure the program
        val conf = new SparkConf()
                                .setAppName("Music Playlist Clustering")
                                .setMaster("local")
                                .set("spark.driver.memory", "2G")
        val sc = new SparkContext(conf)

        case class Song(title: String, artist: String, tags: Set[String]) {
            override def toString: String = title + ", "  + artist
        }

        // Loading songs 
        var songs: RDD[(VertexId, Song)] =
            sc.textFile("./data/song_list.txt").
            map {line => 
                val row = line split '\t'
                val vid = row(0).toLong
                val song =  Song(row(1), row(2), Set.empty)
                (vid, song)
            }

        // Put songs in a graph without edges
        val socialMusic: Graph[Song, Int] = {
            val zeroEdge: RDD[Edge[Int]] = sc.parallelize(Nil)
            Graph(songs, zeroEdge)
        }
            
        // Get the tags
        val tagIter: Iterator[(VertexId, Set[String])] = 
            Source.fromFile("./data/tags.txt").getLines.zipWithIndex.
            map {
                x => 
                val tags = x._1 split ' '
                (x._2.toLong, tags.toSet)
            }
        val tagRDD = sc.parallelize(tagIter.toSeq)

        tagRDD.take(3).foreach(println)

        // Addt the tags into the music graph
        val songsNtags = socialMusic.joinVertices(tagRDD){
            (id, s, ks) => ks.toList match {
                case List("#") => s
                case _         => {
                    val tags: Map[Int, String] = 
                    Source.fromFile("./data/tag_list.txt").getLines().
                    map {
                        line => 
                        val row  = line split ", "
                        row(0).toInt -> row(1)
                    }.toMap

                    val songTags = ks.map(_.toInt) flatMap (tags get)
                    Song(s.title, s.artist, songTags.toSet)
                }
            }   
        }

        songsNtags.vertices.take(3).foreach(println)

        // Add tags to the songs RDD
        songs = songsNtags.vertices

        // Returns true if the Jaccard similarity score exceeds a given threshold
        def similarByTags(one: Song, other: Song, threshold: Double): Boolean = {
            val commonTags = one.tags intersect other.tags
            val combinedTags = one.tags union other.tags
            commonTags.size > combinedTags.size * threshold
        }

        // Returns true if not duplicate songs
        def differentSong(one: Song, other: Song): Boolean = 
            one.title != other.title || one.artist != other.artist 

        // Return the Jaccard similarity measure based on tags
        def similarity(one: Song, other: Song):Double = {
            val numCommonTags = (one.tags intersect other.tags).size
            val numTotalTags = (one.tags union other.tags).size
            numCommonTags.toDouble / numTotalTags.toDouble
        }

        // Create a fully connected graph of songs with jaccard measure as edge attribute
        val similarConnections: RDD[Edge[Double]] = {
            val ss = songs cartesian songs
            val similarSongs = ss filter {
                p => p._1._1 != p._2._1 && 
                similarByTags(p._1._2, p._2._2, 0.7) && 
                differentSong(p._1._2, p._2._2)
            }
            
            similarSongs map {
                p => {
                    val jacIdx = similarity(p._1._2, p._2._2)
                    Edge(p._1._1, p._2._1, jacIdx)
                }
            }
        }
        similarConnections.count
        // This is the resulting graph
        val similarByTagsGraph = Graph(songs, similarConnections)
        similarByTagsGraph.triplets.take(5).foreach(t => println(t.srcAttr + " ~~~ " + t.dstAttr))

        // Filter out songs that have less than 5 tags
        val similarHighTagsGraph = similarByTagsGraph.subgraph(vpred = (id: VertexId, attr: Song) => attr.tags.size > 5)
        similarHighTagsGraph.vertices.count
        similarHighTagsGraph.edges.count
        similarHighTagsGraph.triplets.take(10).foreach(t => println(t.srcAttr + " ~~~ " + t.dstAttr + " => " + t.attr))

        // Calculate the affinity matrix as RDD
        val similarities: RDD[(Long,Long,Double)] = similarHighTagsGraph.triplets.map{t => (t.srcId, t.dstId, t.attr)}

        // Build and run the Power Iteration Clustering model
        val pic = new PowerIterationClustering().setK(7).setMaxIterations(20)
        val clusteringModel = pic.run(similarities)

        // Display raw assignments
        clusteringModel.assignments.foreach { a =>
            println(s"${a.id} -> ${a.cluster}") 
        }

        // Put clustering results into a graph
        val clustering: RDD[(Long, Int)] = clusteringModel.assignments.map(a => (a.id, a.cluster))
        val graph: VertexRDD[Double] = Graph.fromEdges[Double,Double](similarities.map(t => Edge(t._1,t._2,t._3)), 0.0).vertices
        val clusteredSongs: VertexRDD[(Song, Int)] = graph.innerJoin(clustering){ (id, _, cluster) => cluster }.innerJoin(songs){ (id, cluster, s) => (s, cluster)}
        val clusterNScoreGraph = Graph(clusteredSongs, similarities.map(t => Edge(t._1,t._2,t._3)))

        val clusteredSongGraph = clusterNScoreGraph.subgraph(epred = t => t.srcAttr._2 == t.dstAttr._2)

        val clusteredTagsGraph = clusteredSongGraph.mapTriplets(t => t.srcAttr._1.tags intersect t.dstAttr._1.tags)

        // Find common tags for each cluster approximately using Pregel
        val commonTagsByCluster = clusteredTagsGraph.pregel[Set[String]](initialMsg = Set.empty, maxIterations = 10){
            (id, sc, m) => sc, 
            t => Iterator((t.srcId, t.srcAttr._1.tags intersect t.dstAttr._1.tags), 
            (t.dstId, t.srcAttr._1.tags intersect t.dstAttr._1.tags)), (s1, s2) => s1 intersect s2
        }

    }
}


