import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def createSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("Simple Application")
    if (System.getenv("USE_LOCAL_MASTER") == "1") conf.setMaster("local")
    new SparkContext(conf)
  }

  def main(args: Array[String]) {
    val logFile = "data/example.txt" // Should be some file on your system
    val sc = createSparkContext()
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
