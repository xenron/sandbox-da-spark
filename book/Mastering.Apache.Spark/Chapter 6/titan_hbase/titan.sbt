
name := "T i t a n"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core"  % "1.3.1" 

libraryDependencies += "com.cloudera.spark" % "hbase"   % "5-0.0.2" from "file:///home/hadoop/spark/SparkOnHBase-cdh5-0.0.2/target/SparkHBase.jar"

libraryDependencies += "org.apache.hadoop.hbase" % "client"   % "5-0.0.2" from "file:///home/hadoop/spark/SparkOnHBase-cdh5-0.0.2/target/SparkHBase.jar"

// If using CDH, also add Cloudera repo

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

