import java.net.URL
import java.util.regex.Pattern

import com.sun.jersey.spi.StringReader
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.xml.sax.InputSource;
import de.l3s.boilerpipe.extractors;
import java.io.StringReader;
import de.l3s.boilerpipe.sax.HTMLHighlighter;
import org.cyberneko.html.HTMLConfiguration
import scala.collection.JavaConversions._


object SimpleApp {

  def createSparkContext(): SparkContext = {

    val conf = new SparkConf().setAppName("Simple Application")
    conf.set("spark.executor.memory", "100g");
    conf.set("spark.default.parallelism","200");
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
    val sc = createSparkContext()
    val logFile = args(0)
    val min_partitions = args(1).toInt
    val getAnchors= args(2).toBoolean
    //val min_partitions = 30;
    //val logFile = sc.getConf.get("data")
    val files = sc.wholeTextFiles(logFile, min_partitions)
    val words=  files.flatMap(x => x._2.split("WARC/1.0").drop(2))
                .map(doc => doc.substring(doc.indexOf("\n\r", 1+doc.indexOf("\n\r")))).filter(doc => !doc.isEmpty())
                .flatMap(doc =>
                {
                      try
                      {
                        val anchors = List()
                        val textDocument = new BoilerpipeSAXInput(new InputSource(new java.io.StringReader(doc))).getTextDocument()
                        val originalDoc = textDocument.getTextBlocks()
                        val documentContent = extractors.ArticleExtractor.INSTANCE.getText(textDocument)
                         if(getAnchors) {
                           textDocument.getTextBlocks().foreach(hhh=>
                           {
                             var cur_elem = -1;
                             val settedBits = hhh.getContainedTextElements()
                             do
                             {
                               cur_elem = settedBits.nextSetBit(1 + cur_elem)
                               if(textDocument.anchors.containsKey(cur_elem)) {
                                 val cur_href = textDocument.anchors.get(cur_elem);
                                 anchors.+(cur_href)
                               }
                             }
                             while(cur_elem != -1)
                           });
                        }
                        documentContent.split(" ")
                      }
                      catch
                      {
                        case e: Exception=> Array("")
                      }
                })
    val counts = words.map(word => (word, 1)).reduceByKey(_ + _)

    val top_words = counts.top(100)(Ordering.by[(String, Int), Int](_._2))
    top_words.foreach(x => {
      println(x._1 + ":" + x._2)
    })
  }
}
