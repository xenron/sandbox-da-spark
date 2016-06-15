#!/bin/bash

# This script is designed to be used with Apache Spark 1.2.1 and Titan 0.9.0-M2, it uses 
# gremlin and shows that a groovy script can be passed as a parameter

TITAN_HOME=/usr/local/titan/
SCRIPTS_HOME=/home/hadoop/spark/gremlin
GREMLIN_LOG_FILE=$TITAN_HOME/log/gremlin_console.log

GROOVY_SCRIPT=$1

export GREMLIN_LOG_LEVEL="DEBUG"

cd $TITAN_HOME

bin/titan.sh start

bin/gremlin.sh -e  $SCRIPTS_HOME/$GROOVY_SCRIPT  > $GREMLIN_LOG_FILE 2>&1

bin/titan.sh stop

