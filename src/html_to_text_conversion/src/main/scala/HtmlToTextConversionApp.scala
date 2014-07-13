import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object HtmlToTextConversionApp {

  val successExtension: String = ".success"

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val inputDirectory = sc.getConf.get("input")
    val outputDirectory = sc.getConf.get("output")
    val files = filesToProcess(inputDirectory, outputDirectory)
    sc.parallelize(files, 10000).foreach(f => processWarcFile(outputDirectory, f))
  }

  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("HTML to Text Conversion Application")

    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.set("local", "false")
      conf.setMaster("local[*]")
      conf.set("input", "data")
      conf.set("output", "out")
    } else {
      conf.set("local", "true")
      conf.set("input", "file:///mnt/cw12/cw-data")
      //conf.set("input", "hdfs://dco-node121.dco.ethz.ch:54310/cw-data")
      conf.set("output", "hdfs://dco-node121.dco.ethz.ch:54310/ClueWebConverted")
    }

    new SparkContext(conf)
  }

  def processWarcFile(outPath: String, inputPath: String) {
    val fs = FileSystem.get(new Configuration())
    val contents = fs.open(new Path(inputPath))
    val logger = LogManager.getLogger("WarcFileProcessor")
    val processor = new WarcFileProcessor(contents, logger)

    val filePath = inputPath.substring(inputPath.lastIndexOf("ClueWeb12/"))
    val writer: Writer = getFileWriter(outPath + "/" + filePath)
    processor.foreach(doc => writer.append(doc._1, doc._2))
    writer.close()
    getFileWriter(outPath + "/" + filePath + successExtension).close()
  }

  def getFileWriter(outPath: String): Writer = {
    val writer: Writer = {
      val uri = outPath
      val conf = new Configuration()
      val fs = FileSystem.get(URI.create(uri), conf)
      val path = new Path(uri)
      val key = new Text()
      val value = new Text()
      // TODO: fix deprecation warning
      val writer: Writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(), CompressionType.NONE)
      writer
    }
    writer
  }

  def filesToProcess(inputDirectory: String, outputDirectory: String): List[String] = {
    val inputFiles = HadoopFileHelper.listHdfsFiles(new Path(inputDirectory))
      .map(el => el.substring(el.lastIndexOf("ClueWeb12/"))).filter(el => el.endsWith(".warc"))
    val successfulProcessedFiles = HadoopFileHelper.listHdfsFiles(new Path(outputDirectory))
      .map(el => el.substring(el.lastIndexOf("ClueWeb12/"))).filter(el => el.endsWith(successExtension))

    val filesToProcess = inputFiles.filter(el => !successfulProcessedFiles.contains(el + successExtension))
    filesToProcess.map(f => inputDirectory + "/" + f)
  }

}
