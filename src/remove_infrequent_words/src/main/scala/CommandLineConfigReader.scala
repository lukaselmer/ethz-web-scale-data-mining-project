import org.apache.spark.SparkConf

class CommandLineConfigReader(conf: SparkConf, options: Map[String, String]) {

  private var defaultOutput: String = "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined-pruned"
  private var defaultInputCombined: String = "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined"
  private var defaultIntputWordcount: String = "hdfs://dco-node121.dco.ethz.ch:54310/cw-wordcount/wordcounts.txt"

  // Master is not set => use local master, and local data
  if (isLocal) {
    conf.setMaster("local[*]")
    defaultOutput = "out/cw-combined-pruned"
    defaultInputCombined = "data/cw-combined"
    defaultIntputWordcount = "data/cw-wordcount"

    if (defaultOutput == output) {
      scala.reflect.io.Path(defaultOutput).deleteRecursively()
      scala.reflect.io.Path(defaultOutput).createDirectory(failIfExists = true)
    }
  }

  def isCorrect = options.contains("MIN_WORD_COUNT")

  def isLocal: Boolean = !conf.contains("spark.master")

  def printHelp() = {
    println("Usage: <param1>=<value1> <param2>=<value2> ... mahout-submit ...")
    println("Options")
    println("MIN_WORD_COUNT: number, how many times a word has to occur at least to be kept")
    println("[optional] MAX_WORD_COUNT: default: Int.MaxInt")
    println("[optional] OUTPUT: default: " + defaultOutput)
    println("[optional] INPUT_COMBINED: default: " + defaultInputCombined)
    println("[optional] INPUT_WORDCOUNT: default: " + defaultIntputWordcount)
  }

  /**
   * Describes how often one word can appear at maximum (e.g. could be useful for generic stop words removal).
   * If a word appears more often than this amount, it is removed.
   * @return
   */
  def maxWordCount: Int = options.getOrElse("MAX_WORD_COUNT", Int.MaxValue).toString.toInt

  /**
   * Describes how often one word has to appear at minimum.
   * If a word appears less often than this amount, it is removed.
   * @return
   */
  def minWordCount: Int = options.get("MIN_WORD_COUNT").get.toInt

  def output: String = options.getOrElse("OUTPUT", defaultOutput)

  def inputCombined: String = options.getOrElse("INPUT_COMBINED", defaultInputCombined)

  def inputWordcount: String = options.getOrElse("INPUT_WORDCOUNT", defaultIntputWordcount)

  override def toString: String = {
    List(("maxWordCount", maxWordCount), ("minWordCount", minWordCount), ("output", output),
      ("inputCombined", inputCombined), ("inputWordcount", inputWordcount))
      .map(x => x._1 + "=" + x._2).mkString(", ")
  }
}
