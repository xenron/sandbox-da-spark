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
cassConf.setProperty("storage.cassandra.keyspace","titan")

titanGraph = TitanFactory.open(cassConf);

// create traversal and print graph

g = titanGraph.traversal()

g.V().valueMap();

EOF
