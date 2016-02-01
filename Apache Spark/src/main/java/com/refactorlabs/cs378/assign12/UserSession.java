package com.refactorlabs.cs378.assign12;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import java.io.Serializable;

import scala.Tuple2;
import scala.math.Ordering;

import java.util.*;

/**
 * Created by jonghoonlim on 12/1/15.
 * User sessions application for Spark.
 */
public class UserSession implements java.io.Serializable {

    // to keep up with how many unique sessions
    private static Set<Tuple2<String, String>> uniqueSessions = new HashSet<Tuple2<String, String>>();

    public static void main(String[] args) {

        Utils.printClassPath();

        String inputFilename = args[0];
        String outputFilename = args[1];

        // Create a Java Spark context
        SparkConf conf = new SparkConf().setAppName(UserSession.class.getName()).setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the input data
        JavaRDD<String> input = sc.textFile(inputFilename);

        final Random random = new Random();

        // accumulator for total number of events
        final Accumulator<Integer> eventsCount = sc.accumulator(0);
        // accumulator for total number of sessions
        final Accumulator<Integer> sessionCount = sc.accumulator(0);
        // accumulator for OTHER events
        final Accumulator<Integer> otherCount = sc.accumulator(0);
        // accumulator for OTHER events that were discarded
        final Accumulator<Integer> otherDiscardedCount = sc.accumulator(0);

        PairFlatMapFunction<String, Tuple2<String, String>, Event> userMapFunction =
                new PairFlatMapFunction<String, Tuple2<String, String>, Event>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<String, String>, Event>> call(String line) throws Exception {
                        // get all tokens by splitting them by the tab characters
                        String[] tokens = line.split("\\t");

                        List<Tuple2<Tuple2<String, String>, Event>> tupleList = Lists.newArrayList();

                        String userId = tokens[0];
                        String referringDomain = tokens[4];

                        // to keep up with total sessions count
                        if (uniqueSessions.add(new Tuple2<String, String>(userId, referringDomain)))
                            sessionCount.add(1);

                        // get the event and event subtype
                        String eventString = tokens[1];
                        String eventTime = tokens[5];

                        // new Event Object for this userId
                        Event event = new Event();
                        event.eventType = eventString;
                        event.eventTimestamp = eventTime;

                        // output with no filter if not other event
                        String[] notOther = new String[]{"click", "ilmr_cpo", "ilmr_see", "show", "submit", "visit"};
                        boolean matched = false;
                        for (String match : notOther) {
                            if (event.eventType.contains(match)) {
                                tupleList.add(new Tuple2<Tuple2<String,String>, Event>
                                        (new Tuple2<String, String>(userId, referringDomain), event));
                                // accumulator
                                eventsCount.add(1);
                                matched = true;
                                break;
                            }
                        }

                        // is an OTHER event... filter!
                        if (!matched) {
                            int randomValue = random.nextInt(1000) + 1;
                            // 1 in 1000 chance... output 1 out of 1000
                            if (randomValue == 7) {
                                tupleList.add(new Tuple2<Tuple2<String,String>, Event>
                                        (new Tuple2<String, String>(userId, referringDomain), event));
                                // accumulator
                                otherCount.add(1);
                            }
                            // discard and increment the accumulator
                            else {
                                otherDiscardedCount.add(1);
                            }
                            eventsCount.add(1);
                        }

                        return tupleList;
                    }
                };

        // map the sessions
        JavaPairRDD<Tuple2<String, String>, Event> wordWithVerse = input.flatMapToPair(userMapFunction);

        // reduce
        JavaPairRDD<Tuple2<String, String>, Iterable<Event>> listEvents = wordWithVerse.groupByKey();

        // to sort the values
        Function<Iterable<Event>, Iterable<Event>> sortValues =
                new Function<Iterable<Event>, Iterable<Event>>() {
                    @Override
                    public Iterable<Event> call(Iterable<Event> events) throws Exception {

                        Comparator<Event> comparator = new Comparator<Event>() {

                            @Override
                            public int compare(Event event1, Event event2) {
                                return event1.eventTimestamp.compareTo(event2.eventTimestamp);
                            }

                        };

                        List tempList = new ArrayList();
                        for (Event event : events)
                            tempList.add(event);

                        Collections.sort(tempList, comparator);

                        Iterable<Event> result = tempList;

                        return result;
                    }
                };

        JavaPairRDD<Tuple2<String, String>, Iterable<Event>> sortedValues = listEvents.mapValues(sortValues);

        // create a comparator to sort by userIDs
        TupleComparator comparator = new TupleComparator();

        // sort sessions by using the comparator
        JavaPairRDD<Tuple2<String, String>, Iterable<Event>> sortedList = sortedValues.sortByKey(comparator, true, 8);

        sortedList.saveAsTextFile(outputFilename);

        System.out.println("The count for the total number of events is " + eventsCount.value() + ".");
        System.out.println("The count for the total number of sessions is " + sessionCount.value() + ".");
        System.out.println("The count for the number of sessions of type OTHER is " + otherCount.value() + ".");
        System.out.println("The count for the number of discarded sessions of type OTHER is " +
            otherDiscardedCount.value() + ".");

        // Shut down the context
        sc.stop();

    }

    // Comparator to sort the sessions by the UserIds
    public static class TupleComparator implements Comparator<Tuple2<String, String>>, Serializable {

        @Override
        public int compare(Tuple2<String, String> firstTuple2, Tuple2<String, String> secondTuple2) {
            return firstTuple2._1.compareTo(secondTuple2._1);
        }

    }

    private static class Event implements Serializable {
        String eventType;
        String eventTimestamp;
        public String toString() { return "<" + eventType + "," + eventTimestamp + ">";}
    }

}