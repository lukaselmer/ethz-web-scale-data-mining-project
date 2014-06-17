import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.xml.sax.InputSource;
import de.l3s.boilerpipe.extractors;
//import org.cyberneko.html.HTMLConfiguration

object SimpleApp {
  def createSparkContext(): SparkContext = {

    val conf = new SparkConf().setAppName("Simple Application")
    conf.set("spark.executor.memory", "100g");
    conf.set("spark.default.parallelism","200");
    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
      conf.set("data", "/data")
    } else {
      conf.set("data", "/mnt/cw12/cw-data/ClueWeb12_00/0000tw")
    }

    new SparkContext(conf)
  }

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val logFile = sc.getConf.get("data")
    val files = sc.wholeTextFiles(logFile,10)
    val words=  files.flatMap(x => x._2.split("WARC/1.0").drop(2))
                .map(doc => doc.substring(doc.indexOf("\n\r", 1+doc.indexOf("\n\r"))  ) )
                .flatMap(doc => extractors.ArticleExtractor.INSTANCE.getText(doc).split(" "))

    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)

    val top_words = counts.top(100)(Ordering.by[(String, Int), Int](_._2))
    top_words.foreach(x => {
      println(x._1 + ":" + x._2)
    })
  }
}
