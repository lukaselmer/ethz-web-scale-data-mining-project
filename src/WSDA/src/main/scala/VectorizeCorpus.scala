import java.net.URL
import java.util.regex.Pattern
import edu.umd.cloud9.math.Gamma;
import scala.collection.mutable;
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
    conf.set("spark.akka.frameSize","200");
    conf.set("spark.akka.timeout","200");
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
    val input = args(0)
    val stem = args(1).toBoolean
    val output = args(2)
    val sc = createSparkContext();
    //Read vectorized data set
    var vocab = sc.sequenceFile[String, String](input)
                  .flatMap(a => a._2.split(" "));

    if(stem)
      vocab = vocab.map(u => PorterStemmer.stem(u));

    vocab = vocab.distinct()

    vocab.saveAsTextFile(output);
  }
}
