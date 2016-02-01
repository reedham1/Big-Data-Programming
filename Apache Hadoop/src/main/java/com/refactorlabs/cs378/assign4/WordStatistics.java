package com.refactorlabs.cs378.assign4;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 * Created by Jong Hoon Lim on 9/28/15.
 */
public class WordStatistics {
    /*
    * mapper class for word statistics
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        // reuse this word statistics writable
        private static final WordStatisticsWritable wordStatisticsWritable = new WordStatisticsWritable();

        // for the mapper to count the words
        private static final HashMap<String, Long> wordCount = new HashMap<String, Long>();

        // let the mapper get document count, word count, square sum count
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // read in the current paragraph and convert to String
            String paragraph = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(paragraph);
            wordCount.clear();
            while (stringTokenizer.hasMoreTokens()) {
                String token = stringTokenizer.nextToken();

                // store the word in our HashMap
                if (wordCount.containsKey(token)) {
                    wordCount.put(token, wordCount.get(token) + 1L);
                } else {
                    wordCount.put(token, 1L);
                }
            }
            for (String hashKey : wordCount.keySet()) {
                // get the word count
                long count = wordCount.get(hashKey);
                wordStatisticsWritable.initialCount(1L, count, count*count);
                context.write(new Text(hashKey), wordStatisticsWritable);
            }
        }
    }


    /**
     * reducer class to compute the mean and variance
     * change the output value to DoubleArrayWritable to represent mean and variance
     */
    public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

        WordStatisticsWritable wordStatisticsWritable = new WordStatisticsWritable();

        @Override
        public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
                throws IOException, InterruptedException {

            wordStatisticsWritable.reset();

            for (WordStatisticsWritable mapperWSWritable : values) {
                wordStatisticsWritable.updateWritable(mapperWSWritable);
            }

            context.write(key, wordStatisticsWritable);
        }
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "WordStatistics");

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);

        // Set the output key and value types (for map and reduce).
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordStatisticsWritable.class);

        // Set the map, combiner and reduce classes.
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(ReduceClass.class);
        job.setReducerClass(ReduceClass.class);

        // Set the input and output file formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);
    }
}
