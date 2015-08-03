import AssemblyKeys._

assemblySettings


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xds @ _*) => MergeStrategy.discard
  case PathList("plugin.properties", xds @ _*) => MergeStrategy.first
  case PathList("org", "apache", xds @ _*) => MergeStrategy.first
  case PathList("org", "apache","commons","collections", xds @ _*) => MergeStrategy.first
  case PathList("com","esotericsoftware", "minlog", xds @ _*) => MergeStrategy.first
  case PathList("scala","reflect", "api", xds @ _*) => MergeStrategy.first
  case PathList("org", "cyberneko","html", xds @ _*) => MergeStrategy.first
  case PathList("javax", xds @ _*) => MergeStrategy.first
  case x => old(x)
}
}

assemblyOption in assembly ~= { _.copy(includeScala = false) }

assemblyOption in assembly ~= { _.copy(cacheUnzip = false) }

assemblyOption in assembly ~= { _.copy(cacheOutput = false) }
