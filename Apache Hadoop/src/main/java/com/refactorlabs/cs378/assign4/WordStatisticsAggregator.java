package com.refactorlabs.cs378.assign4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import com.refactorlabs.cs378.assign4.WordStatistics.MapClass;
import com.refactorlabs.cs378.assign4.WordStatistics.ReduceClass;


/**
 * Created by Jong Hoon Lim on 9/28/15.
 */
public class WordStatisticsAggregator {
    /*
    * mapper class for word statistics aggregator
    */
    public static class MapClass extends Mapper<Text, Text, Text, WordStatisticsWritable> {

        // turn String into WordStatisticsWritable to pass on to reducer
        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String output = value.toString();
            // turn String into array
            String[] progress = output.split(",");
            long paragraph = Long.valueOf(progress[0]);
            long count = Long.valueOf(progress[1]);
            long squareSum = Long.valueOf(progress[2]);
            double mean = Double.valueOf(progress[3]);
            double variance = Double.valueOf(progress[4]);
            WordStatisticsWritable writable = new WordStatisticsWritable(paragraph, count, squareSum, mean, variance);

            // pass on to reducer
            context.write(new Text(key), writable);
        }
    }

    // use WordStatistics Reducer Class


    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "WordStatisticsAggregator");

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatisticsAggregator.class);

        // Set the output key and value types (for map and reduce).
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordStatisticsWritable.class);

        // Set the map, combiner and reduce classes.
        job.setMapperClass(MapClass.class);
        // Extra-Credit. Use same reducer class as the Word Statistics.
        job.setCombinerClass(WordStatistics.ReduceClass.class);
        job.setReducerClass(WordStatistics.ReduceClass.class);

        // Set the input and output file formats.
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);
    }
}