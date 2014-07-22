import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.spark.{SparkConf, SparkContext}


object RemoveInfrequentWordsApp {

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val inputCombinedDirectory = sc.getConf.get("inputCombined")
    val inputWordcountDirectory = sc.getConf.get("inputWordcount")
    val outputDirectory = sc.getConf.get("output")
    val maximumDictionarySize = sc.getConf.getInt("maximumDictionarySize", 1)


    val keepWords: Set[String] = loadKeepWords(sc, inputWordcountDirectory, maximumDictionarySize)

    val processFileFunction = (inputFile: String) => processFile(inputFile, outputDirectory, keepWords)
    val filesToProcess = HadoopFileHelper.listHdfsFiles(new Path(inputCombinedDirectory)).filter(s => s.endsWith(".combined"))
    if (sc.getConf.getBoolean("local", false))
      filesToProcess.foreach(processFileFunction)
    else
      sc.parallelize(filesToProcess, filesToProcess.length).foreach(processFileFunction)
  }

  def loadKeepWords(sc: SparkContext, inputWordcountDirectory: String, maximumDictionarySize: Int): Set[String] = {
    sc.textFile(inputWordcountDirectory)
      .take(maximumDictionarySize)
      .map(x => x.reverse.split(",").head.replaceFirst("\\)", "").reverse)
      .toList.toSet
  }

  def processWords(text: Text, keepWords: Set[String]): Text = {
    new Text(text.toString.split(" ").filter(word => keepWords.contains(word)).mkString(" "))
  }

  def processFile(inputFile: String, outputDirectory: String, keepWords: Set[String]) {
    val writer = getFileWriter(outputDirectory + "/" + inputFile.split("/").last)
    val key = new Text
    val value = new Text
    val reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(inputFile)))
    while (reader.next(key, value)) writer.append(key, processWords(value, keepWords))

    writer.close
  }

  def getFileWriter(outPath: String): Writer = {
    val writer: Writer = {
      val uri = outPath
      val conf = new Configuration()
      val fs = FileSystem.get(URI.create(uri), conf)
      val path = new Path(uri)
      println(path)
      val key = new Text()
      val value = new Text()
      // TODO: fix deprecation warning
      val writer: Writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(), CompressionType.NONE)
      writer
    }
    writer
  }

  def createSparkContext(): SparkContext = {
    val conf = new SparkConf()

    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
      conf.set("local", "true")
      conf.set("inputCombined", "data/cw-combined")
      conf.set("inputWordcount", "data/cw-wordcount")
      conf.set("output", "out/cw-combined-pruned")
      scala.reflect.io.Path(conf.get("output")).deleteRecursively()
      scala.reflect.io.Path(conf.get("output")).createDirectory(failIfExists = true)
    } else {
      conf.set("local", "false")
      conf.set("inputCombined", "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined")
      conf.set("inputWordcount", "hdfs://dco-node121.dco.ethz.ch:54310/cw-wordcount")
      conf.set("output", "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined-pruned")
    }

    conf.set("minPartitions", "10")
    conf.set("maximumDictionarySize", "10")

    val printConfig = "inputCombined inputWordcount output minPartitions".split(" ").map(k => k + "=" + conf.get(k)).mkString(", ")
    conf.setAppName("Remove Infrequent Words: %s".format(printConfig))

    new SparkContext(conf)
  }


}
