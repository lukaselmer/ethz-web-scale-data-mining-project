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
      conf.set("data", "data/example*")
      conf.set("out", "out")
    } else {
      conf.set("spark.executor.memory", "100g");
      conf.set("spark.default.parallelism", "200");
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      conf.set("spark.kryo.registrator", "MyRegistrator")
      conf.set("data", "file:///mnt/cw12/cw-data/ClueWeb12_00/0000tw")
      conf.set("out", "hdfs:///ClueWebConverted/ClueWeb12_00_0000tw")
    }
    new SparkContext(conf)
  }

  def processWarcFile(outPath: String, inputPath: String, contents: String) {
    val processor = new WarcFileProcessor(contents)

    // Debug code: processor.foreach(doc => println(doc._1 + ": " + doc._2))

    val writer: Writer = {
      val uri = outPath + "/" + inputPath.substring(inputPath.lastIndexOf("data/"))
      val conf = new Configuration()
      val fs = FileSystem.get(URI.create(uri), conf)
      val path = new Path(uri)
      val key = new Text()
      val value = new Text()
      // TODO: fix deprecation warning
      val writer: Writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass())
      writer
    }
    processor.foreach(doc => writer.append(doc._1, doc._2))
  }

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val logFile = sc.getConf.get("data")
    val files = sc.wholeTextFiles(logFile)
    val out = sc.getConf.get("out")
    files.foreach(f => processWarcFile(out, f._1, f._2))
  }
}
