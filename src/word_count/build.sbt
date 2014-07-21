name := "WordCountApp"

version := "1.0"

scalaVersion := "2.10.4"
/* replace it with line below when this issue is resolved: https://issues.apache.org/jira/browse/SPARK-1812
scalaVersion := "2.11.1" */


// Spark

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

// Resolvers

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"


// Unit tests

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test"
