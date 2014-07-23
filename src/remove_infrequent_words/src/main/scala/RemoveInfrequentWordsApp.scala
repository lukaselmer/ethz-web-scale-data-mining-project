import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}
import org.apache.spark.{SparkConf, SparkContext}


object RemoveInfrequentWordsApp {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val configReader = new CommandLineConfigReader(conf, sys.env)
    val sc = createSparkContext(conf, configReader)

    val inputCombinedDirectory = configReader.inputCombined
    val inputWordcountDirectory = configReader.inputWordcount
    val outputDirectory = configReader.output
    val minWordCount = configReader.minWordCount
    val maxWordCount = configReader.maxWordCount

    val keepWords: Set[String] = loadKeepWords(sc, inputWordcountDirectory, minWordCount, maxWordCount)

    val processFileFunction = (inputFile: String) => processFile(inputFile, outputDirectory, keepWords)
    val filesToProcess = HadoopFileHelper.listHdfsFiles(new Path(inputCombinedDirectory)).filter(s => s.endsWith(".combined"))
    sc.parallelize(filesToProcess, filesToProcess.length).foreach(processFileFunction)
  }

  def loadKeepWords(sc: SparkContext, inputWordcountDirectory: String, minWordCount: Int, maxWordCount: Int): Set[String] = {
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
      // why doesn't this work in scala :'-( minWordCount <= countAndWord._1 <= maxWordCount
      .filter(countAndWord => minWordCount <= countAndWord._1 && countAndWord._1 <= maxWordCount)
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

  def createSparkContext(conf: SparkConf, configReader: CommandLineConfigReader): SparkContext = {
    if (!configReader.isCorrect) {
      configReader.printHelp()
      sys.exit(1)
    }

    conf.setAppName("Remove Infrequent Words")

    println("=====================================")
    println("Remove Infrequent Words Configuration")
    println("-------------------------------------")
    println(configReader.toString)
    println("=====================================")

    new SparkContext(conf)
  }


}
