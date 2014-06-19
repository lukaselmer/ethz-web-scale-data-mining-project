name := "SparkDemoProject"

version := "1.0"

scalaVersion := "2.10.4"
/* replace it with line below when this issue is resolved: https://issues.apache.org/jira/browse/SPARK-1812
scalaVersion := "2.11.1" */

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "xerces" % "xercesImpl" % "2.9.1"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.21"

libraryDependencies += "de.l3s.boilerpipe" % "boilerpipe" % "1.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "boilerpipe-m2-repo" at "http://boilerpipe.googlecode.com/svn/repo/"
