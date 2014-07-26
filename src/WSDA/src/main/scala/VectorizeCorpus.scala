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
  /*
  Convert a set of sequence files in text form to a vector space model. The final output is a set of sequence files
  containing the vector space model and another file containing the dictionary.

  The output is a sequence file of key,value pairs, where the key is a document identifier and the value is a string:
    v1:c1 v2:c2 v3:c3 vk:cK
  where v is the index of the token in the dictionary, c is the number of occurence of the term v in the document
   Params
   1-input:             Path to the folder containing a set of sequence files of the dataset.
   3-Filter Threshold:  An integer representing the minimum number of times a token occurs in the data set. Can be used
                        to filter out low frequency terms.
   4-Vocab Output       Path to store the final dictionary
   5-Data Set Output    Path to store the vector space model of the data set.
   */
  def buildVocabulary(args: Array[String]) {
    val input = args(0)
    val stem = args(1).toBoolean
    val filter_threshold = args(2).toInt
    val vocabOutput = args(3)
    val output = args(4)
    val sc = createSparkContext();

    //Read vectorized data set
    var vocab = sc.sequenceFile[String, String](input)
                  .flatMap({ case(document_id , document) => document.split("\\s+") });

    if(stem)
      vocab = vocab.map(token => PorterStemmer.stem(token));

    val vocabFiltered = vocab.filter(token => !token.isEmpty())
                              .map(token => (token.replaceAll("\n","") ,1))
                              .reduceByKey(_ + _)
                              .filter({ case(word, numberOfOccurence) => !word.isEmpty() && numberOfOccurence > filter_threshold })
                              .map({ case (word, numberOfOccurence) => word });

    //Build a hash table of <word, index>
    val dictionary = new mutable.HashMap[String, Int];

    val saved_dictionary = vocabFiltered.collect();

    saved_dictionary.zipWithIndex.foreach({case(word, index) => {
      dictionary.put(word, index);
      saved_dictionary(index) = word + " " + index;
    }})

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
        val content = value.toString().split("\\s+");
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
    def filesToProcess(inputDirectory: String): List[String] = {
    var inputFiles = HadoopFileHelper.listHdfsFiles(new Path(inputDirectory));
    inputFiles
  }

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

}
