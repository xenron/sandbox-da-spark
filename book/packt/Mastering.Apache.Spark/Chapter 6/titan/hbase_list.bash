#!/bin/bash

# This script is designed to be used with Apache Spark 1.2.1 and Titan 0.9.0-M2, it uses 
# gremlin and creates the basic graph

TITAN_HOME=/usr/local/titan/

cd $TITAN_HOME

bin/titan.sh start

# run Titan Spark as a gremlin script

bin/gremlin.sh <<EOF

// create configuration

hbase_prop = '/home/hadoop/spark/gremlin/hbase.prop'

t = TitanFactory.open(hbase_prop)

// create traversal and print graph

g = t.traversal()

g.V().valueMap();

EOF

# stop the titan script

bin/titan.sh stop

