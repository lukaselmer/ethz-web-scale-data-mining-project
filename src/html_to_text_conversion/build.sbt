name := "SparkDemoProject"

version := "1.0"

scalaVersion := "2.10.4"
/* replace it with line below when this issue is resolved: https://issues.apache.org/jira/browse/SPARK-1812
scalaVersion := "2.11.1" */


// Spark, Hadoop, Mahout

//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.0.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.0.0"

libraryDependencies += "org.apache.mahout" % "mahout-core" % "0.9"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

//libraryDependencies += "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "container,test,compile" artifacts Artifact("javax.servlet", "jar", "jar")

// WARC file parser

libraryDependencies += "org.jwat" % "jwat-warc" % "1.0.1"


// Boilerplate:

libraryDependencies += "de.l3s.boilerpipe" % "boilerpipe" % "1.2.0"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.21"

libraryDependencies += "xerces" % "xercesImpl" % "2.11.0"


// Resolvers

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "boilerpipe-m2-repo" at "http://boilerpipe.googlecode.com/svn/repo/"

