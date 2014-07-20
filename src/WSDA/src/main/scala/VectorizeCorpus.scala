import java.net.URI;
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Writer}
import org.apache.hadoop.io.{SequenceFile, Text}
import scala.collection.mutable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.JavaConversions._
import java.io._

object VectorizeCorpus {
  private val successExtension: String = ".success"
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Simple Application")
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
    val HDFS_ROOT = "hdfs://dco-node121.dco.ethz.ch:54310/"
    //val HDFS_ROOT = ""
    val input = HDFS_ROOT + args(0)
    val stem = args(1).toBoolean
    val filter_threshold = args(2).toInt
    val vocabOutput = HDFS_ROOT + args(3)
    val output = HDFS_ROOT + args(4)
    val sc = createSparkContext();

    //Read vectorized data set
    var vocab = sc.sequenceFile[String, String](input+"*/*")
                  .flatMap(a => a._2.split(" "));

    if(stem)
      vocab = vocab.map(u => PorterStemmer.stem(u));

    val vocabFiltered = vocab.filter(v => !v.isEmpty())
                              .map(w => (w,1))
                              .reduceByKey(_ + _)
                              .filter(f=> f._2 > filter_threshold)
                              .map(f => f._1);

    val vocabSize = vocabFiltered.count();
    //Build a hashtable of word_index
    val dictionary = new mutable.HashMap[String, Int];
    var index = 0;

    vocabFiltered.collect().foreach(u => {
      dictionary.put(u, index);
      index += 1;
    })

    val saved_dictionary = dictionary.keys.toList;
    sc.parallelize(saved_dictionary,1).saveAsTextFile(vocabOutput);
    val broadcasted_dictionary = sc.broadcast(dictionary);

    val files = filesToProcess(input)
    sc.parallelize(files, 500).foreach(inputPath =>
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
      while (reader.next(key, value))
      {
        val frequency_table = new mutable.HashMap[Int, Int];
        var emit = new Text();
        val content = value.toString().split(" ");
        content.foreach(w =>
        {
          var cur_word = w;
          if(stem)
            cur_word = PorterStemmer.stem(w);
          if(broadcasted_dictionary.value.contains(cur_word))
          {
            val word_index = broadcasted_dictionary.value.get(cur_word).get;
            if(frequency_table.containsKey(word_index))
              frequency_table.update(word_index, frequency_table.get(word_index).get + 1);
            else
              frequency_table.put(word_index, 1);
          }
        });

        emit = frequency_table
          .map(index_count_pair => index_count_pair._1 + ":" + index_count_pair._2)
          .mkString(" ");
        //Append to the writer
        if(!emit.toString.isEmpty())
          writer.append(key, emit);
      }
      writer.close()
      getFileWriter(output + "/" + filePath + successExtension).close()
    });
  }

  def getFileWriter(outPath: String): Writer = {
    val writer: Writer = {
      val uri = outPath
      val conf = new Configuration()
      val fs = FileSystem.get(URI.create(uri), conf)
      val path = new Path(uri)
      val key = new Text()
      val value = new Text()
      val writer: Writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass(), CompressionType.NONE)
      writer
    }
    writer
  }

  def writeToFile(p: String, s: String): Unit = {
    val pw = new PrintWriter(new File(p))
    try pw.write(s) finally pw.close()
  }
  def filesToProcess(inputDirectory: String): List[String] = {
    var inputFiles = HadoopFileHelper.listHdfsFiles(new Path(inputDirectory));
    inputFiles
  }

}
