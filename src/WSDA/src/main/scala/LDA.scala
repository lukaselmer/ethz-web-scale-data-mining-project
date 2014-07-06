import java.net.URL
import java.util.regex.Pattern
import breeze.linalg._

import edu.umd.cloud9.math.Gamma;
import scala.math;
import org.apache.log4j._;
import com.sun.jersey.spi.StringReader
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.xml.sax.InputSource;
import de.l3s.boilerpipe.extractors;
import java.io.StringReader;
import org.cyberneko.html.HTMLConfiguration
import scala.collection.JavaConversions._
import java.io._

object LDA {
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

  def driver() {
    val sc = createSparkContext()
    var not_converge = true;
    while(not_converge) {
      //normalize columns of lambda
      performIteration()
      not_converge = true;
    }
  }
  def main(args: Array[String]) {
    val t0 = System.currentTimeMillis()
    performIteration()
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
  }
  def writeToFile(p: String, s: String): Unit = {
    val pw = new PrintWriter(new File(p))
    try pw.write(s) finally pw.close()
  }

  def performIteration() {
    val sc = createSparkContext();
    val V = 10475; //Vocabulary size
    val K = 20;  //NUMBER OF Topics
    val DELTA = -1;
    val GAMMA_CONV_ITER = 100;
    val MAX_GLOBAL_ITERATION = 1;//30;
    val ALPHA_CONVERGENCE_THRESHOLD = 0.001;
    val ALPHA_MAX_ITERATION = 1000;
    val ETA = 0.000000000001;
    val DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD = 0.000001;
    val documents = sc.textFile("ap/ap_sample.dat").zipWithIndex().map(cur =>
    {
      val doc = cur._1
      val doc_id = cur._2
      val elems = doc.split(" ");
      var indexes = Array[Int]();
      var counts = Array[Int]();
      /*)
      val document = DenseVector.zeros[Double](V)
      elems.drop(1).foreach(e =>
      {
        var el = e.split(":");
        val index = el(0).toInt
        val count = el(1).toInt;
        document(index) = count
      });
      */
      Tuple2(elems.drop(1),doc_id)
      //Tuple2(document, doc_id)
    }).cache();

    val D = documents.count().toInt;
    val lambda = DenseMatrix.rand[Double](V,K);
    val gamma = DenseMatrix.rand[Double](D,K);
    var alpha = DenseVector.rand[Double](K) // MR.LDA uses 0.001
    val sufficientStats = DenseVector.zeros[Double](K)


    for(global_iteration <- 0 until MAX_GLOBAL_ITERATION) {
      val t0 = System.currentTimeMillis()
      val result = documents.flatMap(cur => {
        val document = cur._1
        val cur_doc = cur._2.toInt
        val phi = DenseMatrix.zeros[Double](V, K);
        var emit = List[((Int, Int), Double)]()
        for (iter <- 0 until GAMMA_CONV_ITER) {
          val sigma = DenseVector.zeros[Double](K)
          for (word_ind <- 0 until document.length) {
            var el = document(word_ind).split(":");
            val v = el(0).toInt
            val count = el(1).toDouble;

            for (k <- 0 until K) {
              phi(v, k) = lambda(v, k) * Math.exp(Gamma.digamma(gamma(cur_doc, k)));
            }
            //normalize rows of phi
            val v_norm = phi(v, ::).t.norm()
            phi(v, ::) := phi(v, ::) :* (1 / v_norm);
            sigma :+= sigma + (phi(v, ::) :* (count)).t;
          }
          gamma(cur_doc, ::) := (alpha + sigma).t;
        }
        for (k <- 0 until K) {
          for (word_ind <- 0 until document.length) {
            var el = document(word_ind).split(":");
            val v = el(0).toInt
            val count = el(1).toDouble;
            emit = emit.+:((k, v), count * phi(v, k))
          }
          val suff_stat = Gamma.digamma(gamma(cur_doc, k)) - Gamma.digamma((sum(gamma(cur_doc, ::).t)))
          emit = emit.+:((k, DELTA), suff_stat)
        }
        emit
      }).reduceByKey(_ + _)
        .collect() //driver
        .foreach(f => {
        if (f._1._2 != DELTA)
          lambda(f._1._2, f._1._1) = ETA + f._2
        else
          sufficientStats(f._1._1) = f._2;
      })

      //row normalize lambda
      val norm = sum(lambda, Axis._1)
      for (v <- 0 until V) {
        lambda(v, ::) := lambda(v, ::) :* (1 / norm(v));
      }

      //Update alpha
      var keepGoing = true;
      var alphaIteration_count = 0;
      while (keepGoing) {
        val gradient = DenseVector.zeros[Double](K)
        val qq = DenseVector.zeros[Double](K)
        for (i <- 0 until K) {
          gradient(i) = D * (Gamma.digamma(sum(alpha)) - Gamma.digamma(alpha(i))) + sufficientStats(i);
          qq(i) = -1 / D * Gamma.trigamma(alpha(i))
        }
        val z_1 = 1 / (D * Gamma.trigamma(sum(alpha)));
        val b = sum(gradient :* qq) / (z_1 + sum(qq));
        val H_g = (gradient - b) :* qq;
        val alpha_new = alpha - H_g;

        val delta_alpha = (alpha_new - alpha) :/ alpha;
        keepGoing = false;
        if (any(delta_alpha :> DEFAULT_ALPHA_UPDATE_CONVERGE_THRESHOLD))
          keepGoing = true;
        if (alphaIteration_count > ALPHA_MAX_ITERATION)
          keepGoing = false;
        alphaIteration_count += 1;
        alpha = alpha_new;
      }
      val t1 = System.currentTimeMillis()
      println("Elapsed time for iteration: " +global_iteration + "---"  + (t1 - t0) + "ms")
    }
    writeToFile("ap/lambda.txt", lambda.toString(V+10, 10000000))
    //print(lambda.toString(V,K))
    //val y = 1;
    //val z = sum(lambda, Axis._1)
  }
}
