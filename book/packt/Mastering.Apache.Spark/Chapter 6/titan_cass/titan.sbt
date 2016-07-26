
name := "Spark Cass"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core"  % "1.3.1" 

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector"  % "1.3.0-M1" from "file:///home/hadoop/spark/titan_cass/spark-cassandra-connector_2.10-1.3.0-M1.jar"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core"  % "2.1.5" from "file:///home/hadoop/spark/titan_cass/cassandra-driver-core-2.1.5.jar"

libraryDependencies += "org.joda"  % "time" % "2.3" from "file:///home/hadoop/spark/titan_cass/joda-time-2.3.jar"

libraryDependencies += "org.apache.cassandra" % "thrift" % "2.1.3" from "file:///home/hadoop/spark/titan_cass/cassandra-thrift-2.1.3.jar"

libraryDependencies += "com.google.common" % "collect" % "14.0.1" from "file:///home/hadoop/spark/titan_cass/guava-14.0.1.jar"

// If using CDH, also add Cloudera repo

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

