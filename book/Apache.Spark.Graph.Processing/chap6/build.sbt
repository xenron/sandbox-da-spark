name := "Flavor Network"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
	"org.apache.spark" %% "spark-graphx" % "1.4.0" % "provided"
)

resolvers ++= Seq(
	"Akka Repository" at "http://repo.akka.io/releases/",
	"Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
	"Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases",
	"Repo Scala" at "http://repo.scala-sbt.org/"
)