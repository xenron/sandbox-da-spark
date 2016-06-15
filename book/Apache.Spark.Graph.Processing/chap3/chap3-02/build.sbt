
name := "Graph Connectedness"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
	"org.apache.spark" %% "spark-graphx" % "1.4.0" % "provided",
	"org.graphstream" % "gs-ui" % "1.2",
	"org.scalanlp" % "breeze-viz_2.10" % "0.9",
	"org.scalanlp" % "breeze_2.10" % "0.9"
)


resolvers ++= Seq(
	"Akka Repository" at "http://repo.akka.io/releases/",
	"Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)


// Configure jar named used with the assembly plug-in
jarName in assembly := "graph-Connectedness-assembly.jar"


// Exclude Scala library (JARs that start with scala- and are included in the binary Scala distribution) 
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyExcludedJars in assembly := { 
  val cp = (fullClasspath in assembly).value
  cp filter {Seq("jcommon-1.0.16.jar", "jfreechart-1.0.13.jar") contains _.data.getName}
}