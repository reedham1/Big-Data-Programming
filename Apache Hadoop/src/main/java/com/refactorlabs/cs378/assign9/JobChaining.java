package com.refactorlabs.cs378.assign9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.refactorlabs.cs378.utils.Utils;

import com.refactorlabs.cs378.sessions.*;
import com.refactorlabs.cs378.assign8.MultipleOutputsFiltering;

/**
 * Created by Jong Hoon Lim on 11/8/15.
 */
public class JobChaining extends Configured implements Tool {

    /*
     * Mappe class to read in the Avro files created by our Assignment 8 code (MultipleOutputsFiltering)
     */
    public static abstract class JobChainingMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>,
            AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {

        // to be used by submitter, cpo, clicker mapper
        public abstract String sessionTypeString();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            Session session = value.datum();

            HashMap<EventSubtype, Integer> eventSubtypeMap = new HashMap<>();

            // iterate through session's events
            for (Event event : session.getEvents()) {

                // if it's a CLICK event, add the subtype to the map
                if (event.getEventType() == EventType.CLICK) {
                    EventSubtype eventSubtype = event.getEventSubtype();
                    Integer count = eventSubtypeMap.get(eventSubtype);

                    // doesn't exist in map yet
                    if (count == null) {
                        eventSubtypeMap.put(eventSubtype, 1);
                    }
                    // does exist in map
                    else
                        eventSubtypeMap.put(eventSubtype, count.intValue() + 1);

                }

            }

            // iterate through our event subtype HashMap and pass on to reducer
            for (EventSubtype subtype : EventSubtype.values()) {
                ClickSubtypeStatisticsKey.Builder reducerKey = ClickSubtypeStatisticsKey.newBuilder();
                ClickSubtypeStatisticsData.Builder reducerData = ClickSubtypeStatisticsData.newBuilder();

                // set the the avro value for the reducer
                reducerData.setSessionCount(1);
                Integer count = eventSubtypeMap.get(subtype);
                int count_total = 0;
                if (count != null)
                    count_total = count.intValue();
                reducerData.setTotalCount(count_total);
                reducerData.setSumOfSquares(count_total * count_total);

                // set the key
                reducerKey.setSessionType(sessionTypeString());
                reducerKey.setClickSubtype(subtype.toString());

                // write to the reducer
                context.write(new AvroKey(reducerKey.build()), new AvroValue(reducerData.build()));
            }
        }

    }

    // mapper for the submitter sessions
    public static class SubmitterMapper extends JobChainingMapper {

        @Override
        public String sessionTypeString() {
            return "submitter";
        }

    }

    // mapper for the cpo sessions
    public static class CPOMapper extends JobChainingMapper {

        @Override
        public String sessionTypeString() {
            return "cpo";
        }

    }

    // mapper for the clicker sessions
    public static class ClickerMapper extends JobChainingMapper {

        @Override
        public String sessionTypeString() {
            return "clicker";
        }

    }

    /*
     * Reducer class to compute the statistics and finalize output
     */
    public static class JobChainingReduceClass
            extends Reducer<AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>,
                        AvroKey<ClickSubtypeStatisticsKey>, AvroValue<ClickSubtypeStatisticsData>> {

        @Override
        public void reduce(AvroKey<ClickSubtypeStatisticsKey> key,
                           Iterable<AvroValue<ClickSubtypeStatisticsData>> values, Context context)
                throws IOException, InterruptedException {

            long sessionCount = 0L;
            long subtypeCount = 0L;
            long sumOfSquares = 0L;

            // gather all counts
            for (AvroValue<ClickSubtypeStatisticsData> builder : values) {
                sessionCount += builder.datum().getSessionCount();
                subtypeCount += builder.datum().getTotalCount();
                sumOfSquares += builder.datum().getSumOfSquares();
            }

            ClickSubtypeStatisticsData.Builder reducerOutput = ClickSubtypeStatisticsData.newBuilder();
            // set the session count, total count, and sum of squares
            reducerOutput.setSessionCount(sessionCount);
            reducerOutput.setTotalCount(subtypeCount);
            reducerOutput.setSumOfSquares(sumOfSquares);
            // compute the mean
            double mean = (double) subtypeCount / sessionCount;
            reducerOutput.setMean(mean);
            // compute the variance
            double variance = (double) sumOfSquares / sessionCount - mean * mean;
            reducerOutput.setVariance(variance);
            context.write(key, new AvroValue(reducerOutput.build()));
        }

    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {

        // input file: s3://utcs378/data/dataSet8.avro
        if (args.length != 2) {
            System.err.println("Usage: JobChaining <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        // Filter and Bin Job
        Job filterAndBinJob = Job.getInstance(conf, "MultipleOutputsFiltering");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        filterAndBinJob.setJarByClass(JobChaining.class);

        // Specify the Mappers
        filterAndBinJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(filterAndBinJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(filterAndBinJob, Session.getClassSchema());
        filterAndBinJob.setMapperClass(MultipleOutputsFiltering.MapClass.class);

        AvroJob.setOutputKeySchema(filterAndBinJob, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(filterAndBinJob, Session.getClassSchema());
        filterAndBinJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        for (SessionType sessionType : SessionType.values()) {
            AvroMultipleOutputs.addNamedOutput(filterAndBinJob, sessionType.getText(), AvroKeyValueOutputFormat.class,
                    Schema.create(Schema.Type.STRING), Session.getClassSchema());
        }
        MultipleOutputs.setCountersEnabled(filterAndBinJob, true);

        // Specify the Reducer
        filterAndBinJob.setNumReduceTasks(0);

        // Grab the input file and output directory from the command line.
        FileInputFormat.setInputPaths(filterAndBinJob, appArgs[0]);
        FileOutputFormat.setOutputPath(filterAndBinJob, new Path(appArgs[1]));

        // initiate the map-reduce job, and wait for completion
        filterAndBinJob.waitForCompletion(true);

        /*
        *******************************************************
         */

        // create jobs for the submitter, sharer, and clicker bin
        Job cpoClickStatsJob = Job.getInstance(conf, "CPOClickStats");
        cpoClickStatsJob.setJarByClass(JobChaining.class);

        // Specify the Mapper
        cpoClickStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        cpoClickStatsJob.setMapperClass(CPOMapper.class);
        AvroJob.setInputKeySchema(cpoClickStatsJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(cpoClickStatsJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(cpoClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(cpoClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());

        // Specity the reducer
        cpoClickStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(cpoClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(cpoClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());
        cpoClickStatsJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        // Specify the combiner
        cpoClickStatsJob.setCombinerClass(JobChainingReduceClass.class);

        cpoClickStatsJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(cpoClickStatsJob, new Path(appArgs[1] + "/" + SessionType.
                CPO.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(cpoClickStatsJob, new Path(appArgs[1] + "/" + SessionType.
                CPO.getText() + "Stats"));
        cpoClickStatsJob.submit();

        /*
        **********************************************************************
         */

        // create jobs for the submitter, sharer, and clicker bin
        Job submitterClickStatsJob = Job.getInstance(conf, "SubmitterClickStats");
        submitterClickStatsJob.setJarByClass(JobChaining.class);

        // Specify the Mapper
        submitterClickStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        submitterClickStatsJob.setMapperClass(SubmitterMapper.class);
        AvroJob.setInputKeySchema(submitterClickStatsJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(submitterClickStatsJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(submitterClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(submitterClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());

        // Specity the reducer
        submitterClickStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(submitterClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(submitterClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());
        submitterClickStatsJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        // Specify the combiner
        submitterClickStatsJob.setCombinerClass(JobChainingReduceClass.class);

        submitterClickStatsJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(submitterClickStatsJob,
                new Path(appArgs[1] + "/" + SessionType.SUBMITTER.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(submitterClickStatsJob,
                new Path(appArgs[1] + "/" + SessionType.SUBMITTER.getText() + "Stats"));
        submitterClickStatsJob.submit();

        /*
        **************************************************************
         */

        // create jobs for the submitter, sharer, and clicker bin
        Job clickerClickStatsJob = Job.getInstance(conf, "ClickerClickStats");
        clickerClickStatsJob.setJarByClass(JobChaining.class);

        // Specify the Mapper
        clickerClickStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        clickerClickStatsJob.setMapperClass(ClickerMapper.class);
        AvroJob.setInputKeySchema(clickerClickStatsJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(clickerClickStatsJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(clickerClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(clickerClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());

        // Specity the reducer
        clickerClickStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(clickerClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(clickerClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());
        clickerClickStatsJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        // Specify the combiner
        clickerClickStatsJob.setCombinerClass(JobChainingReduceClass.class);

        clickerClickStatsJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(clickerClickStatsJob,
                new Path(appArgs[1] + "/" + SessionType.CLICKER.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(clickerClickStatsJob,
                new Path(appArgs[1] + "/" + SessionType.CLICKER.getText() + "Stats"));
        clickerClickStatsJob.submit();

        /*
        ********************************************************
         */

        while ( !submitterClickStatsJob.isComplete()
            || !cpoClickStatsJob.isComplete() ||
                !clickerClickStatsJob.isComplete()) {
            Thread.sleep(5000);
        }

//        // DEBUG **************
//        cpoClickStatsJob.waitForCompletion(true);
//        submitterClickStatsJob.waitForCompletion(true);
//        clickerClickStatsJob.waitForCompletion(true);

        /*
        ********************************************************
         */

        // aggregate job
        Job aggregateClickStatsJob = Job.getInstance(conf, "AggregateClickStats");
        aggregateClickStatsJob.setJarByClass(JobChaining.class);
        aggregateClickStatsJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        aggregateClickStatsJob.setMapperClass(Mapper.class);
        AvroJob.setInputKeySchema(aggregateClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setInputValueSchema(aggregateClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());
        AvroJob.setMapOutputKeySchema(aggregateClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(aggregateClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());

        aggregateClickStatsJob.setCombinerClass(JobChainingReduceClass.class);

        aggregateClickStatsJob.setReducerClass(JobChainingReduceClass.class);
        AvroJob.setOutputKeySchema(aggregateClickStatsJob, ClickSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(aggregateClickStatsJob, ClickSubtypeStatisticsData.getClassSchema());
        aggregateClickStatsJob.setOutputFormatClass(TextOutputFormat.class);
        aggregateClickStatsJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(aggregateClickStatsJob, getInputPath(appArgs[1], SessionType.CPO.getText()));
        FileInputFormat.addInputPath(aggregateClickStatsJob, getInputPath(appArgs[1], SessionType.SUBMITTER.getText()));
        FileInputFormat.addInputPath(aggregateClickStatsJob, getInputPath(appArgs[1], SessionType.CLICKER.getText()));
        FileOutputFormat.setOutputPath(aggregateClickStatsJob, new Path(getOutputDir(appArgs[1], "aggregate")));

        aggregateClickStatsJob.waitForCompletion(true);

        return 0;

    }

    // helper method to read in the input path
    private static Path getInputPath(String baseInputPath, String prefix) {
        return new Path(baseInputPath + "/" + prefix + "Stats" + "/" + "part-r-*.avro");
    }

    // helper method to get the output directory
    private static String getOutputDir(String baseInputPath, String prefix) {
        return baseInputPath + "/" + prefix + "Stats";
    }

    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new Configuration(), new JobChaining(), args);
        System.exit(res);
    }

}