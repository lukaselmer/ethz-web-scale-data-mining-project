import java.net.URL
import java.util.regex.Pattern
import edu.umd.cloud9.math.Gamma
import org.apache.log4j.LogManager
import java.net.URI;

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}

import scala.collection.mutable
import scala.math;
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.xml.sax.InputSource;
import java.io.StringReader;
import scala.collection.JavaConversions._
import java.io._

object VectorizeCorpus {
  private val successExtension: String = ".success"
  private val topDirectoryNameInput: String = "cw-data/"
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Simple Application")
    //conf.set("spark.executor.memory", "10g");
    conf.set("spark.default.parallelism","200");
    conf.set("spark.akka.frameSize","2000");
    conf.set("spark.akka.timeout","2000");
    conf.set("spark.worker.timeout", "2000")
    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
      conf.set("data", "data/sample.warc")
    } else {
      conf.set("data", "/mnt/cw12/cw-data/ClueWeb12_00/")
    }

    new SparkContext(conf)
  }


  def main(args: Array[String]) {
    buildVocabulary(args)
  }

  def buildVocabulary(args: Array[String]) {
    val logger = LogManager.getLogger("Vectorize Corpus")

    val HDFS_ROOT = "hdfs://dco-node121.dco.ethz.ch:54310/"
    //val HDFS_ROOT = ""
    val input = HDFS_ROOT + args(0)
    val stem = args(1).toBoolean
    val vocabOutput = HDFS_ROOT + args(2)
    val output = HDFS_ROOT + args(3)
    val sc = createSparkContext();
    //Read vectorized data set
    var vocab = sc.sequenceFile[String, String](input)
                  .flatMap(a => a._2.split(" "));

    if(stem)
      vocab = vocab.map(u => PorterStemmer.stem(u));

    vocab = vocab.filter(v => !v.isEmpty()).distinct();
    val vocabSize = vocab.count();
    //Build a hashtable of word_index
    val dictionary = new mutable.HashMap[String, Int];
    var index = 0;

    logger.error("VOCAB Size: " + vocabSize );

    vocab.collect().foreach(u => {
      dictionary.put(u, index);
      index += 1;
    })

    val saved_dictionary = dictionary.toList;
    sc.parallelize(saved_dictionary).saveAsTextFile(vocabOutput);
    sc.broadcast(dictionary);

    val filess = filesToProcess(input, "src")
    val processWarcFileFunction = (filename: String) => processWarcFile(output, filename)
    sc.parallelize(filess).foreach(inputPath =>
    {
      val fs = FileSystem.get(new Configuration())
      val last_index = inputPath.lastIndexOf("/")
      val second_index = inputPath.lastIndexOf("/", last_index-1)
      val filePath =  inputPath.substring(second_index + 1)

      val conf = new Configuration()
      val key = new Text()
      val value = new Text()

      val reader = new SequenceFile.Reader(fs, new Path(inputPath), conf);
      val writer: Writer = getFileWriter(output+ "/" + filePath)
      val frequency_table = new mutable.HashMap[Int, Int];
      while (reader.next(key, value))
      {
        var emit = new Text();
        val content = value.toString().split(" ");
        content.foreach(w =>
        {
          var cur_word = w;
          if(stem)
            cur_word = PorterStemmer.stem(w);
          if(!cur_word.isEmpty())
          {
            val word_index = dictionary.get(cur_word).get;
            if(frequency_table.containsKey(word_index))
              frequency_table.update(word_index, frequency_table.get(word_index).get + 1);
            else
              frequency_table.put(word_index, 1);
          }
        });

        frequency_table.foreach(f => {
          emit = emit + " " + f._1 + ":" + f._2;
        });
        //Append to the writer
        writer.append(key, emit);
      }
      writer.close()
      getFileWriter(output + "/" + filePath + successExtension).close()
    });

    //read
    //var files = sc.sequenceFile[String, String](input).flatMap(f => f._2.split(" ").map(w => (f._1, w)));
    /*
    val files = sc.sequenceFile[String, String](input);

    val parse_files = files.mapPartitionsWithIndex((partitionIndex,partition) => {
    val output_files = partition.map(f =>
    {
      val file_name = f._1;
      var emit = file_name;
      val content = f._2.split(" ");

      val frequency_table = new mutable.HashMap[Int, Int];

      content.foreach(w =>
      {
        var cur_word = w;
        if(stem)
          cur_word = PorterStemmer.stem(w);
        if(!cur_word.isEmpty())
        {
          val word_index = dictionary.get(cur_word).get;
          if(frequency_table.containsKey(word_index))
            frequency_table.update(word_index, frequency_table.get(word_index).get + 1);
          else
            frequency_table.put(word_index, 1);
        }
      });

      frequency_table.foreach(f => {
        emit = emit + " " + f._1 + ":" + f._2;
      });
      emit;
    });
    writeToFile(output + "/" + partitionIndex, output_files.toList.mkString("\n"))
    Iterator();
  });
  parse_files.count();
  */
  }

  def processWarcFile(outPath: String, inputPath: String) {
    val fs = FileSystem.get(new Configuration())
    val contentStream = fs.open(new Path(inputPath))

    val conf = new Configuration()
    val key = new Text()
    val value = new Text()

    val reader = new SequenceFile.Reader(fs, new Path(inputPath), conf);
    while (reader.next(key, value))
    {

    }
    //read the file
    //val processor = new WarcFileProcessor(contentStream, logger)

    val filePath = inputPath.substring(inputPath.lastIndexOf(topDirectoryNameInput)).replaceFirst(topDirectoryNameInput, "")
    val writer: Writer = getFileWriter(outPath + "/" + filePath)
    //processor.foreach(doc => writer.append(doc._1, doc._2))
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


  def writeToFile(p: String, s: String): Unit = {
    val pw = new PrintWriter(new File(p))
    try pw.write(s) finally pw.close()
  }
  def filesToProcess(inputDirectory: String, topDirectoryNameInput: String): List[String] = {
    var inputFiles = HadoopFileHelper.listHdfsFiles(new Path(inputDirectory));
    inputFiles
    /*
    inputFiles = inputFiles.map(el => el.substring(el.lastIndexOf(topDirectoryNameInput)).replaceFirst(topDirectoryNameInput, ""))
      .filter(el => el.endsWith(".warc"))
    inputFiles.map(f => inputDirectory + "/" + f)
    */
  }

}
