package com.refactorlabs.cs378.assign11;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.StringTokenizer;
import java.util.HashSet;

/**
 * Created by jonghoonlim on 11/23/15.
 * Inverted Index application for Spark.
 */
public class InvertedIndex {

    public static void main(String[] args) {
        Utils.printClassPath();

        String inputFilename = args[0];
        String outputFilename = args[1];

        // Create a Java Spark context
        SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data
        JavaRDD<String> input = sc.textFile(inputFilename);

        // word to verse pair
        PairFlatMapFunction<String, String, String> wordMapFunction =
                new PairFlatMapFunction<String, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(String s) throws Exception {
                        // count towards the first space which is verse name
                        String verse = s.substring(0, s.indexOf(" "));

                        // get the rest of the passage
                        String line = s.substring(s.indexOf(" ") + 1);
                        // remove punctuation from the line
                        line = line.replaceAll("[^a-zA-Z0-9'\\s]+","");
                        // input line to lower case
                        line = line.toLowerCase();

                        StringTokenizer tokenizer = new StringTokenizer(line);
                        HashSet<String> words = new HashSet<String>();
                        List<Tuple2<String, String>> tupleList = Lists.newArrayList();

                        // read the line after the verse
                        while (tokenizer.hasMoreTokens()) {
                            String currentWord = tokenizer.nextToken();

                            // if the hashset does not contain the word, add the word and verse to tuplelist
                            if (!words.contains(currentWord)) {
                                words.add(currentWord);
                                tupleList.add(new Tuple2<String, String>(currentWord, verse));
                            }

                        }
                        return tupleList;
                    }
                };


        JavaPairRDD<String, String> wordWithVerse = input.flatMapToPair(wordMapFunction);
        JavaPairRDD<String, Iterable<String>> wordWithIndex = wordWithVerse.groupByKey();
        JavaPairRDD<String, Iterable<String>> wordWithSortedIndex = wordWithIndex.sortByKey();

        wordWithSortedIndex.saveAsTextFile(outputFilename);

        // Shut down the context
        sc.stop();
    }

}
