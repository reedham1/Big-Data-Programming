package com.refactorlabs.cs378.assign3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Jong Hoon Lim on 9/21/15.
 */

public class InvertedIndex {

    /*
     * Mapper class to analyze each passage
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, StringArrayWritable> {


        // use HashSet since it doesn't allow duplicate values
        private HashSet<String> words = new HashSet<String>();

        @Override
        public void map(LongWritable mapKey, Text value, Context context)
                throws IOException, InterruptedException {

            // read each verse passage
            String passage = value.toString().trim();
            // don't do anything for empty space between passages
            if (passage.equals("")) {
                return;
            }

            // count towards the first space which is verse name
            String getVerse = passage.substring(0, passage.indexOf(" "));
            StringWritable verse = new StringWritable(getVerse);
            StringArrayWritable strArrayWritable = new StringArrayWritable(new StringWritable[]{verse});

            // get the rest of the passage
            passage = passage.substring(passage.indexOf(" ") + 1);
            // get rid of all punctuation
            passage = passage.replaceAll("[^a-zA-Z0-9'\\s]+","");
            passage = passage.toLowerCase();
            StringTokenizer stringTokenizer = new StringTokenizer(passage);

            // clear the HashSet for next passage
            words.clear();

            // get words for this verse and add them to HashSet
            while (stringTokenizer.hasMoreTokens()) {
                String word = stringTokenizer.nextToken();
                words.add(word);
            }

            // pass on to Reducer
            for (String word : words) {
                context.write(new Text(word), strArrayWritable);
            }
        }
    }

    /**
     * Reducer class, combine all the verses for the word
     */
    public static class ReduceClass extends Reducer<Text, StringArrayWritable, Text, StringArrayWritable> {

        public void reduce(Text key, Iterable<StringArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            // use an ArrayList to easily convert to StringArrayWritable later
            ArrayList<StringWritable> arrListValues = new ArrayList<StringWritable>();

            // with the given StringArrayWritable passed from the Mapper
            for (StringArrayWritable strArrayWritable : values) {
                // declare as writable
                Writable[] verses = strArrayWritable.get();
                // add all verses for current word
                for (Writable verse : verses) {
                    arrListValues.add((StringWritable)verse);
                }
            }

            // final output
            context.write(key, new StringArrayWritable(arrListValues));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "InvertedIndex");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(InvertedIndex.class);

        // Set the output key and value types (for map and reduce).
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringArrayWritable.class);

        // Set the map, combiner and reduce classes.
        job.setMapperClass(MapClass.class);
        //job.setCombinerClass(CombinerClass.class);
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
