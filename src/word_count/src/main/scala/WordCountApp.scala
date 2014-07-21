import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val inputDirectory = sc.getConf.get("input")
    val outputFile = sc.getConf.get("output")

    sc.sequenceFile[Text, Text](inputDirectory, classOf[Text], classOf[Text], 1)
      .flatMap(_._2.toString.trim.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(false, 1)
      .saveAsTextFile(outputFile)
  }

  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Word Count Application")

    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.set("local", "true")
      conf.setMaster("local[*]")
      conf.set("input", "data/cw-converted/ClueWeb12_00/")
      conf.set("output", "out/cw-wordcount")
      scala.reflect.io.Path("out").deleteRecursively()
      scala.reflect.io.Path("out").createDirectory(failIfExists = true)
    } else {
      conf.set("local", "false")
      conf.set("input", "hdfs://dco-node121.dco.ethz.ch:54310/cw-converted")
      conf.set("output", "hdfs://dco-node121.dco.ethz.ch:54310/cw-wordcount")
    }

    new SparkContext(conf)
  }


}
