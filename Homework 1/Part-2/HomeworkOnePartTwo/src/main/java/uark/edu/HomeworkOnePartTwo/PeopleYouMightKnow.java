package uark.edu.HomeworkOnePartTwo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

//Tested in Eclipse IDE.  Setup my build process to execute 1) mvn package 2) custom delete script for the output directory
//My run configurations were for a Java application, and my arguments were "input.txt output" 
public class PeopleYouMightKnow {
	static final String INPUT_FILE_NAME = "sociNet.txt";
	static final String OUTPUT_FILE_NAME = "output";
	static final String JAR_PATH = "C:\\Users\\Ryan\\Documents\\GitHub\\Big-Data-Analytics-and-Management\\Homework 1\\Part-2\\HomeworkOnePartTwo\\target";
	
	static final Integer[] findList = {922, 8940, 8943, 9013, 9010, 9011, 9018, 9297, 9930, 9963};
	
	public static void main(String[] args) throws Exception {
		if(args.length != 0) {
			throw new Exception("Usage: PeopleYouMightKnow");
		}

		// Command-line Configuration
		//SparkConf conf = new SparkConf();
		//JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Eclipse Debug Configuration
		JavaSparkContext sc = new JavaSparkContext("local", "PeopleYouMightKnow", System.getProperty("SPARK_HOME"), JAR_PATH);
		
		JavaRDD<String> lines = sc.textFile(INPUT_FILE_NAME);
		
		// Step 1 - Create a complete adjacency list of ((Person, Person), Relationship#)
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> adjacencyList = lines.flatMapToPair(l -> {
			List<Tuple2<Tuple2<Integer, Integer>, Integer>> mapResults = new ArrayList<>();	
			int indexOfTab = l.indexOf('\t');
			String keyStr = l.substring(0, indexOfTab);
			
			// Grab the array of all neighbors to the current line key.
			List<String> friendList = Arrays.asList(l.substring(indexOfTab + 1).trim().split(","));
			
			// Add it to the tuple list we are creating for this key.
			for(int i = 0; i < friendList.size(); i++) {
				String friendStr = friendList.get(i);
				
				if(isInteger(keyStr) && isInteger(friendStr)) {
					int key = Integer.parseInt(keyStr);
					int friend = Integer.parseInt(friendStr);
					
					// Create the tuples where the smallest key comes first (to easily reduce duplicate pairings)
					// Friends = 0, as they are adjacent to the key friend.
					Tuple2<Integer,Integer> friendPair;
					if(key <= friend) {
						friendPair = new Tuple2<>(key, friend);
					}
					else {
					    friendPair = new Tuple2<>(friend, key);
					}
					mapResults.add(new Tuple2<>(friendPair, 1));
					
					// Find all the pairs of suggested friends that we can retrieve from this line.
					// Suggested friends = 1, as you travel one friend to get to them
					for(int j = i + 1; j < friendList.size(); j++) {
						String mutualFriendStr = friendList.get(j);
						
						if(isInteger(mutualFriendStr)) {
							int mutualFriend = Integer.parseInt(mutualFriendStr);
							
							Tuple2<Integer,Integer> suggestedFriendPair;
							if(friend <= mutualFriend) {
								suggestedFriendPair = new Tuple2<>(friend, mutualFriend);
							}
							else {
								suggestedFriendPair = new Tuple2<>(mutualFriend, friend);
							}
							
							mapResults.add(new Tuple2<>(suggestedFriendPair, 1));
						}
					}
				}
			}
			
			return mapResults.iterator();
		});
		
		// Step 2 - Group the relationship levels together, keyed on the <Person,Person> KVP.
		JavaPairRDD<Tuple2<Integer,Integer>, Iterable<Integer>> allRelationshipsGrouped = adjacencyList.groupByKey();
		
		// Step 3 - Mark out pairs who are already friends (0).  Also count the number of unique mutual friends they have
		JavaPairRDD<Tuple2<Integer,Integer>, Integer> markAndCount = allRelationshipsGrouped.mapToPair(r -> {
			Iterable<Integer> relationshipList = r._2;
			int count = 0;
			for(int level : relationshipList) {
				if(level == 0) {
					count = 0;
					break;
				}
				count += level;
			}
			return new Tuple2<Tuple2<Integer,Integer>, Integer>(r._1, count);
		});
		
		// Step 4 - Filter out pairs who are already friends
		markAndCount.filter(f -> (f._2 != 0));
		JavaPairRDD<Tuple2<Integer,Integer>, Integer> uniqueFriends = markAndCount.filter(f -> (f._2 != 0));
		
		// Step 5 - Create friend recommendation pairs for every person.  We need to add the reverse keys for the other pair's reduction.
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> recommendationsUngrouped = uniqueFriends.flatMapToPair(p -> {
			List<Tuple2<Integer, Tuple2<Integer,Integer>>> mapResults = new ArrayList<>();
			Tuple2<Integer, Integer> keyFriendRankPair = new Tuple2<>(p._1._2, p._2);
			Tuple2<Integer, Integer> friendKeyRankPair = new Tuple2<>(p._1._1, p._2);
			mapResults.add(new Tuple2(p._1._1, keyFriendRankPair));
			mapResults.add(new Tuple2(p._1._2, friendKeyRankPair));
			
			return mapResults.iterator();
		});
		
		// Step 6 - Reduce pairs on their key person
		JavaPairRDD<Integer, Iterable<Tuple2<Integer,Integer>>> recommendationsGrouped = recommendationsUngrouped.groupByKey();
		
		// Step 7 - Build top 10 recommendations
		JavaPairRDD<Integer, List<Integer>> recommendations = recommendationsGrouped.mapToPair(r -> {
			List<Tuple2<Integer,Integer>> unsortedList = new ArrayList<>();
			for(Tuple2<Integer,Integer> item : r._2) {
				unsortedList.add(new Tuple2<Integer,Integer>(item._1,item._2));
			}
			
			unsortedList.sort(new Comparator<Tuple2<Integer,Integer>>(){

				@Override
				public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
					return o2._2.compareTo(o1._2);
				}
				
			});
			List<Integer> topFriendsList = new ArrayList<>();
			for(int i = 0; i < 10 && i < unsortedList.size(); i++) {
				topFriendsList.add(unsortedList.get(i)._1);
			}
			return new Tuple2<>(r._1, topFriendsList);
			
		});
		
		recommendations.saveAsTextFile(OUTPUT_FILE_NAME);

		// Old approach that did not work
		// Step 2 - Based on a single mutual friend, generate pairings of suggested friends.  I.E. A->B->C has A->C as the pairing.
		//adjacencyList.
		
		/*JavaPairRDD<Integer, Tuple2<Integer,Integer>> joinResult = adjacencyList.join(adjacencyList).filter(new Function<Tuple2<Integer,Tuple2<Integer,Integer>>, Boolean>() {

			@Override
			public Boolean call(Tuple2<Integer, Tuple2<Integer, Integer>> v1) throws Exception {
				int key = v1._1;
				int ass_1 = v1._2._1;
				int ass_2 = v1._2._2;

				if(ass_1 >= ass_2) {
					return false;
				}
				else 
					return true;
			}
			
		});
		
		JavaPairRDD<Tuple2<Integer,Integer>, Integer> rearrange = joinResult.mapToPair(j -> new Tuple2<>(new Tuple2<Integer,Integer>(j._2._1, j._2._2), 1));
		JavaPairRDD<Tuple2<Integer,Integer>, Integer> totalMutal = rearrange.reduceByKey((n1, n2) -> n1 + n2);
		
		totalMutal.saveAsTextFile(OUTPUT_FILE_NAME);
		*/
		
		/*
		JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.replaceAll("[^a-zA-Z ]", "").toLowerCase().split(" ")).iterator())
									 .filter(i -> !i.isEmpty());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w.substring(0, 1).toLowerCase(),1));

		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((n1, n2) -> n1 + n2);
		counts.saveAsTextFile(OUTPUT_FILE_NAME);
		*/
		
		
		sc.stop();
	}
	
	public static boolean isInteger(String s) {
		int radix = 10;
	    if(s.isEmpty()) return false;
	    for(int i = 0; i < s.length(); i++) {
	        if(i == 0 && s.charAt(i) == '-') {
	            if(s.length() == 1) return false;
	            else continue;
	        }
	        if(Character.digit(s.charAt(i),radix) < 0) return false;
	    }
	    return true;
	}
}
