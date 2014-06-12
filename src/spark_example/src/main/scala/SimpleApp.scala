import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Simple Application")

    // Master is not set => use local master, and local data
    if (!conf.contains("spark.master")) {
      conf.setMaster("local[*]")
      conf.set("data", "data/example.txt")
    } else {
      conf.set("data", "/mnt/cw12/cw-data/ClueWeb12_00/0000tw")
    }
    new SparkContext(conf)
  }

  def main(args: Array[String]) {
    val sc = createSparkContext()
    val logFile = sc.getConf.get("data")
    val lines = sc.textFile(logFile)
    val counts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    val top_words = counts.top(100)(Ordering.by[(String, Int), Int](_._2))

    top_words.foreach(x => {
      println(x._1 + ":" + x._2)
    })
  }
}
