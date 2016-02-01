package com.refactorlabs.cs378.assign2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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

public class WordStatistics {
    /*
    * mapper class for word statistics
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, LongArrayWritable> {

        // for the mapper to count the words
        private static final HashMap<String, Long> wordCount = new HashMap<String, Long>();
        private static final LongArrayWritable longAW = new LongArrayWritable();
        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Count the current paragraph
            context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);
            // read in the current paragraph and convert to String
            String paragraph = value.toString();
            // tokenize line
            StringTokenizer stringTokenizer = new StringTokenizer(paragraph, "=_\";:.,?[! ");
            // clear map for next paragraph and reuse
            wordCount.clear();

            // read in each token in current paragraph
            while (stringTokenizer.hasMoreTokens()) {
                // get next token
                String token = stringTokenizer.nextToken().toLowerCase();
                // remove punctuation
                token = token.replaceAll("[^a-zA-Z0-9'\\s]+","");

                if (wordCount.containsKey(token)) {
                    wordCount.put(token, wordCount.get(token) + 1L);
                } else {
                    wordCount.put(token, 1L);
                }
            }

            // go through the HashMap and write to LongArrayWritable to pass to Combiner class
            for (String hashKey : wordCount.keySet()) {
                long count = wordCount.get(hashKey);
                // get square to find variance later
                long squared = count * count;
                long[] neededValues = new long[]{1L, count, squared};
                longAW.setValues(neededValues);
                context.write(new Text(hashKey), longAW);
            }
        }
    }

    /*
     * Combiner class that sums up the paragraph count, the word count, and the square of the word count
     * using the LongArrayWritable from the mapper class
     */
    public static class CombinerClass extends Reducer<Text, LongArrayWritable, Text, LongArrayWritable> {

        private LongArrayWritable longAW = new LongArrayWritable();
        // reuse the reduce class for combiner
        public void combiner(Text key, Iterable<LongArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            // variables to combine the paragraph count, word count, squared count
            long totalParaCount = 0;
            long totalWordCount = 0;
            long totalSqCount = 0;

            // combine the paragraph count, word count, squared count
            for (LongArrayWritable current : values) {
                long[] currentWord = current.getValues();
                totalParaCount += currentWord[0];
                totalWordCount += currentWord[1];
                totalSqCount += currentWord[2];
            }

            // LongArrayWritable to pass on to the reducer
            long[] neededValues = new long[]{totalParaCount, totalWordCount, totalSqCount};
            longAW.setValues(neededValues);
            context.write(key, longAW);
        }
    }

    /**
     * reducer class to compute the mean and variance
     * change the output value to DoubleArrayWritable to represent mean and variance
     */
    public static class ReduceClass extends Reducer<Text, LongArrayWritable, Text, DoubleArrayWritable> {

        private DoubleArrayWritable doubleAW = new DoubleArrayWritable();
        @Override
        public void reduce(Text key, Iterable<LongArrayWritable> values, Context context)
                throws IOException, InterruptedException {

            // variables to calculate final answers
            double paraCount = 0;
            double mean = 0;
            double variance = 0;

            for (LongArrayWritable current : values) {
                long[] currentWord = current.getValues();
                paraCount += currentWord[0];
                mean += currentWord[1];
                variance += currentWord[2];
            }
            // calculate mean of word occurence in paragraph that it appears
            mean = mean / paraCount;
            // calculate variance of word
            variance = (variance / paraCount) - (mean * mean);
            // DoubleArrayWritable to correctly represent the values

            double[] representVal = new double[]{paraCount, mean, variance};
            doubleAW.setValues(representVal);
            // write out the paragraph count, mean of word, variance of word
            context.write(key, doubleAW);
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
        job.setOutputValueClass(DoubleArrayWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongArrayWritable.class);

        // Set the map, combiner and reduce classes.
        job.setMapperClass(MapClass.class);
        job.setCombinerClass(CombinerClass.class);
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