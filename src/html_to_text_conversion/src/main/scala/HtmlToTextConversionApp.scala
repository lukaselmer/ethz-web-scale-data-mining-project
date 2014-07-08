import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.serializer.KryoSerializer
//import org.jwat.warc.WarcReaderFactory
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import java.io._

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PrintWriter])
  }
}

object HtmlToTextConversionApp {
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("HTML to Text Conversion Application")

    // Master is not set => use local master, and local data

    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
      conf.set("data", "data/example1.warc2")
      conf.set("out", "out")
    } else {
      conf.set("spark.executor.memory", "100g");
      conf.set("spark.default.parallelism", "200");
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //conf.set("spark.kryo.registrator", "MyRegistrator")
      //conf.set("data", "file:///mnt/cw12/cw-data")
      conf.set("data", "/mnt/cw12/cw-data/ClueWeb12_00/*")
      //conf.set("out", "hdfs://dco-node121:54310/ClueWebConverted/")
      conf.set("out", "/local/home/lzhong/ClueWebConverted/")
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
      val writer: Writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(), CompressionType.NONE)
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
