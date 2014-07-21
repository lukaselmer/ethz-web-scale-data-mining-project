import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class TermCounter {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: TermCounter <threshold> <dir>");
			System.exit(1);
		}

		//Set up Spark context.
		SparkConf sparkConf = new SparkConf().setAppName("TermCounter");
		sparkConf.set("spark.executor.memory", "100g");
		sparkConf.set("spark.default.parallelism","200");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		//Read all files in args[0] line by line.
		JavaRDD<String> lines = ctx.textFile(args[1]+"*");

		//Split lines into individual words.
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		//Map words to tuples <word,1>.
		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s.toLowerCase(), 1);
			}
		});

		//Aggregate tuples to global count.
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		//Print result to console.
		List<Tuple2<String, Integer>> output = counts.collect();
        Comparator<Tuple2<String, Integer>> comparator = new TupleComparator();
		PriorityQueue<Tuple2<String, Integer>> ranked = new PriorityQueue<Tuple2<String, Integer>>(200, comparator);
		Tuple2<String, Integer> tmp;
        ranked.clear();
		for (Tuple2<String,Integer> tuple : output) {
			if (tuple._1.length() < 30 && tuple._2() > Integer.parseInt(args[0])){
                if(ranked.size()<100) {
                    ranked.add(tuple);
                }
                else {
                    tmp = ranked.peek();
                    if(tmp._2() < tuple._2()) {
                        ranked.poll();
                        ranked.add(tuple);
                    }
                }
			}
		}
		for (Tuple2<String,Integer> tuple : ranked) {
            System.out.println(tuple._1() + ": " + tuple._2());
		}

		//Destroy context.
		ctx.stop();
	}

}


