name := "SparkDemoProject"

version := "1.0"

scalaVersion := "2.10.4"
/* replace it with line below when this issue is resolved: https://issues.apache.org/jira/browse/SPARK-1812
scalaVersion := "2.11.1" */

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "xerces" % "xercesImpl" % "2.9.1"

libraryDependencies += "net.sourceforge.nekohtml" % "nekohtml" % "1.9.21"

//libraryDependencies += "de.l3s.boilerpipe" % "boilerpipe" % "1.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.4.0"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.2"

libraryDependencies  ++= Seq(
  // other dependencies here
  "org.scalanlp" % "breeze_2.10" % "0.7",
  // native libraries are not included by default. add this if you want them (as of 0.7)
  // native libraries greatly improve performance, but increase jar sizes.
  "org.scalanlp" % "breeze-natives_2.10" % "0.7"
)

libraryDependencies += "edu.umd" %"cloud9" % "1.4.9"

resolvers ++= Seq(
  // other resolvers here
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

//resolvers += "boilerpipe-m2-repo" at "http://boilerpipe.googlecode.com/svn/repo/"
