import org.apache.spark.graphx._
import org.apache.spark.rdd._
import breeze.linalg.SparseVector
import scala.io.Source
import scala.math.abs
type Feature = breeze.linalg.SparseVector[Int]

// This takes the ego.feat file and creates a Map of feature
// The vertexId is created by hashing the string identifier to a Long 
val featureMap: Map[Long, Feature] = 
	Source.fromFile("./data/ego.feat").
			getLines().
			map{ line => 
				val row = line split ' '
				val key = abs(row.head.hashCode.toLong)
				val feat: Feature = SparseVector(row.tail.map(_.toInt))
				(key, feat)
			}.toMap


val edges: RDD[Edge[Int]] = 
  sc.textFile("./data/ego.edges").
     map {line => 
      	val row = line split ' '
      	val srcId = abs(row(0).hashCode.toLong)
      	val dstId = abs(row(1).hashCode.toLong)
      	val numCommonFeats = featureMap(srcId) dot featureMap(dstId)
      	Edge(srcId, dstId, numCommonFeats)
     }

val vertices:  RDD[(VertexId, Feature)] = 
	sc.textFile("./data/ego.edges").
		map{line => 
			val key = abs(line.hashCode.toLong)
			(key, featureMap(key))
}

val egoNetwork: Graph[Int,Int] = Graph.fromEdges(edges, 1)

egoNetwork.edges.filter(_.attr == 3).count()
egoNetwork.edges.filter(_.attr == 2).count()
egoNetwork.edges.filter(_.attr == 1).count()


