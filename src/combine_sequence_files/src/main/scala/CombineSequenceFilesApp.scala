import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{Text, SequenceFile}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.spark.{SparkConf, SparkContext}

object CombineSequenceFilesApp {

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val inputDirectory = sc.getConf.get("input")
    val outputDirectory = sc.getConf.get("output")
    val directoriesToProcess = HadoopFileHelper.listHdfsDirs(new Path(inputDirectory))
    val processDirectoryFunction = (inputDirectory: String) => processDirectory(inputDirectory, outputDirectory)

    if (sc.getConf.getBoolean("local", false))
      directoriesToProcess.foreach(processDirectoryFunction)
    else
      sc.parallelize(directoriesToProcess, directoriesToProcess.length).foreach(processDirectoryFunction)
  }

  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("HTML to Text Conversion Application")

    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.set("local", "true")
      conf.setMaster("local[*]")
      conf.set("input", "data/cw-converted")
      conf.set("output", "out/cw-combined")
      scala.reflect.io.Path("out/cw-combined").deleteRecursively()
      scala.reflect.io.Path("out/cw-combined").createDirectory(failIfExists = true)
    } else {
      conf.set("local", "false")
      conf.set("input", "hdfs://dco-node121.dco.ethz.ch:54310/cw-converted")
      conf.set("output", "hdfs://dco-node121.dco.ethz.ch:54310/cw-combined")
    }

    new SparkContext(conf)
  }

  def processDirectory(inputDirectory: String, outputDirectory: String) {
    val files = HadoopFileHelper.listHdfsFiles(new Path(inputDirectory)).filter(s => s.endsWith(".warc"))

    val writer = getFileWriter(outputDirectory + "/" + inputDirectory.split("/").last + ".combined")
    val key = new Text
    val value = new Text
    for (f <- files) {
      val reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(f)))
      while (reader.next(key, value)) writer.append(key, value)
    }
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

}
