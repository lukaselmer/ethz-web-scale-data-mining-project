import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object HtmlToTextConversionApp {

  private val successExtension: String = ".success"
  private val topDirectoryNameInput: String = "cw-data/"
  private val topDirectoryNameOutput: String = "ClueWebConvertedClean2/"

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val inputDirectory = sc.getConf.get("input")
    val outputDirectory = sc.getConf.get("output")
    val files = filesToProcess(inputDirectory, outputDirectory)
    val processWarcFileFunction = (filename: String) => processWarcFile(outputDirectory, filename)

    if (sc.getConf.getBoolean("local", false))
      files.foreach(processWarcFileFunction)
    else
      sc.parallelize(files, 10000).foreach(processWarcFileFunction)
  }

  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("HTML to Text Conversion Application")

    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.set("local", "true")
      conf.setMaster("local[*]")
      conf.set("input", "data/cw-data")
      conf.set("output", "out/ClueWebConverted")
      scala.reflect.io.Path("out/ClueWebConverted").deleteRecursively()
      scala.reflect.io.Path("out/ClueWebConverted").createDirectory(failIfExists = true)
    } else {
      conf.set("local", "false")
      conf.set("input", "hdfs://dco-node121.dco.ethz.ch:54310/cw-data")
      conf.set("output", "hdfs://dco-node121.dco.ethz.ch:54310/ClueWebConvertedClean2")
    }

    new SparkContext(conf)
  }

  def processWarcFile(outPath: String, inputPath: String) {
    val fs = FileSystem.get(new Configuration())
    val contentStream = fs.open(new Path(inputPath))
    val logger = LogManager.getLogger("WarcFileProcessor")
    val processor = new WarcFileProcessor(contentStream, logger)

    val filePath = inputPath.substring(inputPath.lastIndexOf(topDirectoryNameInput)).replaceFirst(topDirectoryNameInput, "")
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
    // TODO: refactor this
    val inputFiles = HadoopFileHelper.listHdfsFiles(new Path(inputDirectory))
      .map(el => el.substring(el.lastIndexOf(topDirectoryNameInput)).replaceFirst(topDirectoryNameInput, ""))
      .filter(el => el.endsWith(".warc"))
    val successfulProcessedFiles = HadoopFileHelper.listHdfsFiles(new Path(outputDirectory))
      .map(el => el.substring(el.lastIndexOf(topDirectoryNameOutput)).replaceFirst(topDirectoryNameOutput, ""))
      .filter(el => el.endsWith(successExtension))

    val filesToProcess = inputFiles.filter(el => !successfulProcessedFiles.contains(el + successExtension))
    filesToProcess.map(f => inputDirectory + "/" + f)
  }

}
