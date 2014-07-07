import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.spark.{SparkConf, SparkContext}

object HtmlToTextConversionApp {
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("HTML to Text Conversion Application")

    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
      conf.set("data", "data")
    } else {
      conf.set("data", "/mnt/cw12/cw-data/ClueWeb12_00/0000tw")
    }
    new SparkContext(conf)
  }

  def processWarcFile(inputPath: String, contents: String) {
    val writer: Writer = initSequenceFileWriter
    val processor = new WarcFileProcessor(contents)
    processor.iterator

    writer.append("asodfi", "asodijfoaijewf awefoijewoifj awoiefjoijew")

  }


  def initSequenceFileWriter: Writer = {
    val uri = "/ClueWebConverted"
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(uri), conf)
    val path = new Path(uri)
    val key = new Text()
    val value = new Text()
    val writer: Writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass())
    writer
  }

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val logFile = sc.getConf.get("data")
    val files = sc.wholeTextFiles(logFile)
    files.foreach(f => processWarcFile(f._1, f._2))

    //val counts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    //val top_words = counts.top(100)(Ordering.by[(String, Int), Int](_._2))
    //top_words.foreach(x => {
    //  println(x._1 + ":" + x._2)
    //})
  }
}
