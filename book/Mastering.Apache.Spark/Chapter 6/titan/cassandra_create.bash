#!/bin/bash

# This script is designed to be used with Apache Spark 1.2.1 and Titan 0.9.0-M2, it uses 
# gremlin and creates the basic graph

TITAN_HOME=/usr/local/titan/

cd $TITAN_HOME

# run Titan Spark as a gremlin script

bin/gremlin.sh <<EOF

// create configuration

cassConf = new BaseConfiguration();
cassConf.setProperty("storage.backend","cassandra");
cassConf.setProperty("storage.hostname","hc2r1m2");
cassConf.setProperty("storage.port","9160")
cassConf.setProperty("storage.keyspace","titan")

titanGraph = TitanFactory.open(cassConf);

// define the graph schema
// 
//  see http://s3.thinkaurelius.com/docs/titan/0.9.0-M2/titan-hadoop-tp3.html

schema = titanGraph.openManagement();

// define vertex labela

schema.makeVertexLabel("person").make()

// define property keys

nameProp = schema.makePropertyKey('pname').dataType(String.class).make();
ageProp  = schema.makePropertyKey('page').dataType(String.class).make();

// define edge labels

schema.makeEdgeLabel("relationship").make()

// define indices

schema.buildIndex('nameIdx',Vertex.class).addKey(nameProp).buildCompositeIndex();
schema.buildIndex('ageIdx',Vertex.class).addKey(ageProp).buildCompositeIndex();

// commit schema changes

schema.commit();

// create vertices

v1=titanGraph.addVertex('person', '1');
v1.property('pname', 'Mike');
v1.property('page', '48');

v2=titanGraph.addVertex('person', '2');
v2.property('pname', 'Sarah');
v2.property('page', '45');

v3=titanGraph.addVertex('person', '3');
v3.property('pname', 'John');
v3.property('page', '25');

v4=titanGraph.addVertex('person', '4');
v4.property('pname', 'Jim');
v4.property('page', '53');

v5=titanGraph.addVertex('person', '5');
v5.property('pname', 'Kate');
v5.property('page', '22');

v6=titanGraph.addVertex('person', '6');
v6.property('pname', 'Flo');
v6.property('page', '52');

// create edges

v6.addEdge("Sister", v1)
v1.addEdge("Husband", v2)
v2.addEdge("Wife", v1)
v5.addEdge("Daughter", v1)
v5.addEdge("Daughter", v2)
v3.addEdge("Son", v1)
v3.addEdge("Son", v2)
v4.addEdge("Friend", v1)
v1.addEdge("Father", v5)
v1.addEdge("Father", v3)
v2.addEdge("Mother", v5)
v2.addEdge("Mother", v3)

titanGraph.tx().commit();

titanGraph.close()

EOF
