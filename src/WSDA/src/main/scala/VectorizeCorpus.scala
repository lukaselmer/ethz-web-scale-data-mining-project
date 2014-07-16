import java.net.URL
import java.util.regex.Pattern
import edu.umd.cloud9.math.Gamma
import org.apache.log4j.LogManager
;
import scala.collection.mutable
;
import scala.math;
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.xml.sax.InputSource;
import java.io.StringReader;
import scala.collection.JavaConversions._
import java.io._

object VectorizeCorpus {
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Simple Application")
    conf.set("spark.executor.memory", "10g");
    conf.set("spark.default.parallelism","200");
    conf.set("spark.akka.frameSize","2000");
    conf.set("spark.akka.timeout","2000");
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
    sc.parallelize(saved_dictionary, 1).saveAsTextFile(vocabOutput);
    sc.broadcast(dictionary);
    //read
    var files = sc.sequenceFile[String, String](input).flatMap(f => f._2.split(" ").map(w => (f._1, w)));
    if(stem)
      files = files
                .map(k => (k._1, PorterStemmer.stem(k._2)))

    val word_counts = files
                      .filter(u => !u._2.isEmpty())
                      .map(k => (k._1, dictionary.get(k._2)))
                      .map(k => ( k, 1)).reduceByKey(_ + _);

    val output_files = word_counts
      .groupBy(a => a._1._1)
      .map(f => f._1 + " " + f._2.map(u => u._1._2.get + ":" + u._2).mkString(" "));

    output_files.saveAsTextFile(output);
  }
}
