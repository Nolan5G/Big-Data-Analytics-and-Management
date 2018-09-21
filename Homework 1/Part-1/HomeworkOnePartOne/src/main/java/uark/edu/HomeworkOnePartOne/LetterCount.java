package uark.edu.HomeworkOnePartOne;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

// Tested in Eclipse IDE.  Setup my build process to execute 1) mvn package 2) custom delete script for the output directory
// My run configurations were for a Java application, and my arguments were "input.txt output" 
public class LetterCount {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			throw new Exception("Usage: LetterCount input.txt outputDirectory");
		}
		
		// If it does not work, I recommend doing the whole DOS command / Bash commands w below uncommented.
//		SparkConf conf = new SparkConf();
//      JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaSparkContext sc = new JavaSparkContext("local", "lettercount", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<String> lines = sc.textFile(args[0]);
		JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.replaceAll("[^a-zA-Z ]", "").toLowerCase().split(" ")).iterator())
									 .filter(i -> !i.isEmpty());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w.substring(0, 1).toLowerCase(),1));

		// More verbose manner.  More prone to errors related to changing parameter values.  Stick to your lambdas, kids.
//		JavaRDD<String> words = lines.flatMap(
//				new FlatMapFunction<String, String>() {
//					public Iterator<String> call(String l) throws Exception {
//						if(!l.isEmpty()) {
//							l = l.replaceAll("[^a-zA-Z\\s]", "");
//							
//							return Arrays.asList(l.split(" ")).iterator();
//						}
//						return ;
//					}
//					
//				});
		
//		JavaPairRDD<String, Integer> pairs = words.mapToPair(
//				new PairFunction<String, String, Integer>() {
//					public Tuple2<String, Integer> call(String w) throws Exception {
//						return new Tuple2<>(w.substring(0, 1),1);
//					}
//				});
		
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((n1, n2) -> n1 + n2);
		
		counts.saveAsTextFile(args[1]);
		sc.stop();
	}
}
