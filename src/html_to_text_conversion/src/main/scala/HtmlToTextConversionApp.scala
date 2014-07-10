import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

//import org.apache.spark.serializer.KryoSerializer
//import org.jwat.warc.WarcReaderFactory

object HtmlToTextConversionApp {
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("HTML to Text Conversion Application")

    // Master is not set => use local master, and local data

    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
      conf.set("data", "data/example*")
      conf.set("out", "out")
    } else {
      conf.set("data", "file:///mnt/cw12/cw-data/*")
      conf.set("out", "hdfs://dco-node121:54310/ClueWebConverted")
      //conf.set("out", "/disk3/user_work/runs/convert_all2")
    }

    new SparkContext(conf)
  }

  def processWarcFile(outPath: String, inputPath: String, contents: String) {
    val logger = LogManager.getLogger("WarcFileProcessor")
    val processor = new WarcFileProcessor(contents, logger)

    // Debug code: processor.foreach(doc => println(doc._1 + ": " + doc._2))

    val writer: Writer = {
      val uri = outPath + "/" + inputPath.substring(inputPath.lastIndexOf("data/"))
      val conf = new Configuration()
      val fs = FileSystem.get(URI.create(uri), conf)
      val path = new Path(uri)
      val key = new Text()
      val value = new Text()
      // TODO: fix deprecation warning
      val writer: Writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(), CompressionType.NONE)
      writer
    }
    processor.foreach(doc => writer.append(doc._1, doc._2))
    writer.close()
  }

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val logFile = sc.getConf.get("data")
    val files = sc.wholeTextFiles(logFile, 500)
    val out = sc.getConf.get("out")
    files.foreach(f => processWarcFile(out, f._1, f._2))
  }
}
