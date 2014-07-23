import org.apache.spark.SparkConf

class CommandLineConfigReader(conf: SparkConf, options: Map[String, String]) {
  private val defaultOutput: String = "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined-pruned"
  private val defaultInputCombined: String = "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined"
  private val defaultIntputWordcount: String = "hdfs://dco-node121.dco.ethz.ch:54310/cw-wordcount/wordcounts.txt"

  def isCorrect = options.contains("MIN_WORD_COUNT")

  def isLocal: Boolean = !conf.contains("spark.master")

  def printHelp() = {
    println("Usage: <param1>=<value1> <param2>=<value2> ... mahout-submit ...")
    println("Options")
    println("MIN_WORD_COUNT: number, how many times a word has to occur at least to be kept")
    println("[optional] OUTPUT: default: " + defaultOutput)
    println("[optional] INPUT_COMBINED: default: " + defaultInputCombined)
    println("[optional] INPUT_WORDCOUNT: default: " + defaultIntputWordcount)
  }

  def minWordCount: Int = {
    options.get("MIN_WORD_COUNT").get.toInt
  }

  def output: String = {
    options.getOrElse("OUTPUT", defaultOutput)
  }

  def inputCombined: String = {
    options.getOrElse("INPUT_COMBINED", defaultInputCombined)
  }

  def inputWordcount: String = {
    options.getOrElse("INPUT_WORDCOUNT", defaultIntputWordcount)
  }
}
