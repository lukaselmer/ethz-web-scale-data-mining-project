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
    val minWordCount = sc.getConf.getInt("minWordCount", Int.MaxValue)


    val keepWords: Set[String] = loadKeepWords(sc, inputWordcountDirectory, minWordCount)

    val processFileFunction = (inputFile: String) => processFile(inputFile, outputDirectory, keepWords)
    val filesToProcess = HadoopFileHelper.listHdfsFiles(new Path(inputCombinedDirectory)).filter(s => s.endsWith(".combined"))
    if (sc.getConf.getBoolean("local", false))
      filesToProcess.foreach(processFileFunction)
    else
      sc.parallelize(filesToProcess, filesToProcess.length).foreach(processFileFunction)
  }

  def loadKeepWords(sc: SparkContext, inputWordcountDirectory: String, minWordCount: Int): Set[String] = {
    def extractCountAndWord = (x: String) => {
      // x will have the form of "(234,word)", without spaces
      val countAndWord = x.split(",")
      val count = countAndWord.head.substring(1).toInt
      // Handle words with commas
      val word = countAndWord.tail.mkString(",").reverse.substring(1).reverse
      (count, word)
    }

    // https://en.wikipedia.org/wiki/Longest_word_in_English
    // http://www.webcitation.org/66sSvZqYP
    // Longest non-coined word in a major dictionary
    val maxWordLength = 30

    sc.textFile(inputWordcountDirectory)
      .take(minWordCount)
      .map(extractCountAndWord)
      .filter(_._1 >= minWordCount)
      .map(_._2)
      .filter(_.length <= maxWordLength)
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

    val configReader = new CommandLineConfigReader(conf, sys.env)

    if (!configReader.isCorrect) {
      configReader.printHelp()
      sys.exit(1)
    }

    // Master is not set => use local master, and local data
    if (configReader.isLocal) {
      conf.setMaster("local[*]")
      conf.set("local", "true")
      conf.set("inputCombined", "data/cw-combined")
      conf.set("inputWordcount", "data/cw-wordcount")
      conf.set("output", "out/cw-combined-pruned")
      scala.reflect.io.Path(conf.get("output")).deleteRecursively()
      scala.reflect.io.Path(conf.get("output")).createDirectory(failIfExists = true)
    } else {
      conf.set("local", "false")
      conf.set("inputCombined", configReader.inputCombined)
      conf.set("inputWordcount", configReader.inputWordcount)
      conf.set("output", configReader.output)
    }

    conf.set("minWordCount", configReader.minWordCount.toString)

    val printConfig = "inputCombined inputWordcount output minWordCount".split(" ").map(k => k + "=" + conf.get(k)).mkString(", ")
    conf.setAppName("Remove Infrequent Words: %s".format(printConfig))

    new SparkContext(conf)
  }


}
