#!/bin/bash

# This script is designed to be used with Apache Spark 1.2.1 and Titan 0.9.0-M2, it uses 
# gremlin and creates the basic graph

TITAN_HOME=/usr/local/titan/

cd $TITAN_HOME

# run Titan Spark as a gremlin script

bin/gremlin.sh <<EOF

// create configuration

hBaseConf = new BaseConfiguration();
hBaseConf.setProperty("storage.backend","hbase");
hBaseConf.setProperty("storage.hostname","hc2r1m2,hc2r1m3,hc2r1m4");
hBaseConf.setProperty("storage.hbase.ext.hbase.zookeeper.property.clientPort","2181")
hBaseConf.setProperty("storage.hbase.table","titan")
titanGraph = TitanFactory.open(hBaseConf);

// define properties

manageSys = titanGraph.openManagement();
nameProp = manageSys.makePropertyKey('name').dataType(String.class).make();
ageProp  = manageSys.makePropertyKey('age').dataType(String.class).make();
manageSys.buildIndex('nameIdx',Vertex.class).addKey(nameProp).buildCompositeIndex();
manageSys.buildIndex('ageIdx',Vertex.class).addKey(ageProp).buildCompositeIndex();

manageSys.commit();

// create verices

v1=titanGraph.addVertex(label, '1');
v1.property('name', 'Mike');
v1.property('age', '48');

v2=titanGraph.addVertex(label, '2');
v2.property('name', 'Sarah');
v2.property('age', '45');

v3=titanGraph.addVertex(label, '3');
v3.property('name', 'John');
v3.property('age', '25');

v4=titanGraph.addVertex(label, '4');
v4.property('name', 'Jim');
v4.property('age', '53');

v5=titanGraph.addVertex(label, '5');
v5.property('name', 'Kate');
v5.property('age', '22');

v6=titanGraph.addVertex(label, '6');
v6.property('name', 'Flo');
v6.property('age', '52');

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

EOF
