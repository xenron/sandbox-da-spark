#!/bin/bash

SPARK_HOME=/opt/cloudera/parcels/CDH
SPARK_LIB=$SPARK_HOME/lib
SPARK_BIN=$SPARK_HOME/bin
SPARK_SBIN=$SPARK_HOME/sbin
SPARK_JAR=$SPARK_LIB/spark-assembly-1.2.0-cdh5.3.3-hadoop2.5.0-cdh5.3.3.jar

# Exception in thread "main" org.apache.spark.SparkException: Found both 
# spark.executor.extraClassPath and SPARK_CLASSPATH. Use only the former.
#
# SPARK_CLASSPATH=$SPARK_LIB:$SPARK_CLASSPATH
# export SPARK_CLASSPATH

PATH=$SPARK_BIN:$PATH
PATH=$SPARK_SBIN:$PATH
export PATH


cd $SPARK_BIN

./spark-submit \
  --class $1 \
  --master spark://hc2nn.semtech-solutions.co.nz:7077  \
  --executor-memory 85m \
  --total-executor-cores 50 \
  /home/hadoop/spark/databricks/target/scala-2.10/data-bricks_2.10-1.0.jar 

