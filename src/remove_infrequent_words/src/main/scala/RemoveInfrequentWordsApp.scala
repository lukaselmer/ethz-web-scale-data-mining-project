import org.apache.spark.{SparkConf, SparkContext}

object RemoveInfrequentWordsApp {

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val inputCombinedDirectory = sc.getConf.get("inputCombined")
    val inputWordcountDirectory = sc.getConf.get("inputWordcount")
    val outputDirectory = sc.getConf.get("output")

    //TODO: implement this
  }

  def createSparkContext(): SparkContext = {
    val conf = new SparkConf()

    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
      conf.set("inputCombined", "data/cw-combined")
      conf.set("inputWordcount", "data/cw-wordcount")
      conf.set("output", "out/cw-combined-pruned")
      scala.reflect.io.Path(conf.get("output")).deleteRecursively()
      scala.reflect.io.Path(conf.get("output")).createDirectory(failIfExists = true)
      conf.set("minPartitions", "10")
    } else {
      conf.set("inputCombined", "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined")
      conf.set("inputWordcount", "hdfs://dco-node121.dco.ethz.ch:54310/cw-wordcount")
      conf.set("output", "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined-pruned")
      conf.set("minPartitions", "10")
    }

    val printConfig = "inputCombined inputWordcount output minPartitions".split(" ").map(k => k + "=" + conf.get(k)).mkString(", ")
    conf.setAppName("Remove Infrequent Words: %s".format(printConfig))

    new SparkContext(conf)
  }


}
