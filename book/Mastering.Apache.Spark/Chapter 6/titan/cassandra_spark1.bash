#!/bin/bash

# This script is designed to be used with Apache Spark 1.2.1 and Titan 0.9.0-M2, it uses 
# gremlin and shows a simple graph of the gods create using an embedded script

TITAN_HOME=/usr/local/titan/

# start the titan script

cd $TITAN_HOME

bin/titan.sh start

# run Titan Spark as a gremlin script

bin/gremlin.sh <<EOF

  t = TitanFactory.open('cassandra.properties')
  GraphOfTheGodsFactory.load(t)
  t.close()

EOF

# stop the titan script

bin/titan.sh stop

