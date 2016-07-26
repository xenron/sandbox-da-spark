
// this groovy example just tries to use GraphFactory along with a 
// properties file 

import com.thinkaurelius.titan.core.*
import com.thinkaurelius.titan.core.titan.*
import org.apache.tinkerpop.*


// define the cass property files

// cass_prop = '/home/hadoop/spark/gremlin/cassandra.properties'
cass_prop = '/home/hadoop/spark/gremlin/hadoop-load.properties'

// create a graph factory

graph = GraphFactory.open('/home/hadoop/spark/gremlin/hadoop-load2.properties')

// g1 = graph.traversal(computer(SparkGraphComputer))

graph.traversal().V().count()

// g1.V().valueMap()

