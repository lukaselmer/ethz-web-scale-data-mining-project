name := "SparkDemoProject"

version := "1.0"

scalaVersion := "2.10.4"
/* replace it with line below when this issue is resolved: https://issues.apache.org/jira/browse/SPARK-1812
scalaVersion := "2.11.1" */

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.apache.mahout" %% "mahout-core" % "0.9"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

